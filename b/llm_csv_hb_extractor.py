#!/usr/bin/env python3
"""
INPUTS:
  - ~/iec/pcXX/district_handbooks/eb_table_extracts/*.pdf
  - ~/iec/pcXX/district_handbooks_xii_b/eb_table_extracts/*.pdf
OUTPUTS:
  - ~/iec/pcXX/district_handbooks/eb_table_extracts/*.csv
  - ~/iec/pcXX/district_handbooks_xii_b/eb_table_extracts/*.csv

Script to extract tables from PDF files using Gemini LLM API.
- Loads prompt template from file
- Processes all PDFs in a directory
- Maintains a log of processed files
- Handles errors and retries
"""
import sys
import os
import shutil
import re
import json
import numpy as np
import pandas as pd
import time
from pathlib import Path
from io import StringIO
from datetime import datetime
from ddlpy.utils import IEC, TMP
from dotenv import load_dotenv
from pathlib import Path
from google import genai
from google.genai import types
import PyPDF2
import sys, shlex
import argparse

# Add project directory to sys.path for imports
sys.path.append(os.path.expanduser("~/ddl/pc01_llm_extract"))

# Load environment variables from .env file
load_dotenv()  # Loads variables from .env file
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')

# Initialize Gemini client
client = genai.Client()

def extract_pdf_tables(pdf_path, output_dir):
    """
    Extract tables from a PDF file using Gemini LLM API.
    For long PDFs, split into 20-page chunks and process each chunk separately.
    Args:
        pdf_path (str): Path to the PDF file.
        output_dir (str): Directory to save output (not used directly here).
    Returns:
        str: Extracted CSV text.
    """
    print(f"\n[INFO] Extracting tables from: {pdf_path}")
    
    # Load prompt template from file
    prompt_template_path = "./prompt_template.txt"
    with open(prompt_template_path, 'r') as f:
        prompt_template = f.read()
    print(f"[DEBUG] Loaded prompt template from {prompt_template_path}")

    pdf_filepath = Path(pdf_path)

    # Read PDF and determine number of pages
    with open(pdf_filepath, 'rb') as pdf_file:
        reader = PyPDF2.PdfReader(pdf_file)
        num_pages = len(reader.pages)
    print(f"[INFO] PDF has {num_pages} pages.")

    chunk_size = 20
    csv_chunks = []

    # loop over the PDF, 20 pages at a time
    for start_page in range(0, num_pages, chunk_size):
        end_page = min(start_page + chunk_size, num_pages)
        print(f"[INFO] Processing pages {start_page+1} to {end_page}...")
        
        # Extract chunk pages and write to in-memory PDF
        writer = PyPDF2.PdfWriter()
        with open(pdf_filepath, 'rb') as pdf_file:
            reader = PyPDF2.PdfReader(pdf_file)
            for i in range(start_page, end_page):
                writer.add_page(reader.pages[i])
            from io import BytesIO
            chunk_pdf_bytes = BytesIO()
            writer.write(chunk_pdf_bytes)
            chunk_pdf_bytes.seek(0)
            chunk_pdf_data = chunk_pdf_bytes.read()

        # LLM extraction for this chunk
        success = False
        attempt = 0
        while not success:
            attempt += 1
            try:
                print(f"[INFO] Sending request to Gemini LLM (attempt {attempt}) for chunk {start_page+1}-{end_page}...")
                response = client.models.generate_content(
                    model="gemini-2.5-flash",
                    contents=[
                        types.Part.from_bytes(
                            data=chunk_pdf_data,
                            mime_type='application/pdf',
                        ),
                        prompt_template,
                    ],
                    config=types.GenerateContentConfig(
                        temperature=0,
                        thinking_config=types.ThinkingConfig(thinking_budget=-1))
                )
                print("[INFO] Received response from Gemini LLM.")
                success = True
            except Exception as e:
                print(f"[ERROR] Error generating content from Gemini LLM: {e}")
                print("[INFO] Retrying in 30 seconds...")
                time.sleep(30)

        # Extract CSV from markdown block in response
        csv_text = response.text
        print("[DEBUG] Raw response text received for chunk.")
        
        # Clean up the CSV text to remove any markdown formatting
        csv_text = re.sub(r'```csv\n', '', csv_text)
        csv_text = re.sub(r'```', '', csv_text)
        print("[DEBUG] Cleaned CSV text for chunk.")
        csv_chunks.append(csv_text)

    # Combine all CSV chunks
    print("[INFO] Combining CSV chunks...")
    
    # Use pandas to concatenate, handling headers only once
    dfs = []
    for i, chunk in enumerate(csv_chunks):
        buff = StringIO(chunk)
        if i == 0:
            df = pd.read_csv(buff)
        else:
            df = pd.read_csv(buff, header=0)
        dfs.append(df)
    combined_df = pd.concat(dfs, ignore_index=True)
    
    # Convert back to CSV string
    final_csv = combined_df.to_csv(index=False)
    print("[INFO] Combined CSV ready.")
    return final_csv


def main():
    """
    Main function to process all PDF files in the input directory.
    """
    print("[INFO] Starting PDF table extraction process...")
    
    # Get all PDF files in the input directory
    p = argparse.ArgumentParser()
    p.add_argument("--series", required=True, choices=["pc01","pc11","pc91","pc51"])

    # folder containing csv to scan
    p.add_argument("--pdf_root", required=True)

    # --- handle Stata passing a single-argument blob ---
    argv = sys.argv[1:]
    if len(argv) == 1 and ("--" in argv[0] or " " in argv[0]):
        argv = shlex.split(argv[0])

    args = p.parse_args(argv)


    pdf_dir = Path(args.pdf_root)
    input_dir  = pdf_dir / f"eb_table_extracts/"
    output_dir = pdf_dir / f"eb_table_extracts/"

    print(f"[INFO] Input directory: {input_dir}")
    print(f"[INFO] Output directory: {output_dir}")

    # generate a list of all pdf files in the input dir
    pdf_files = [f for f in os.listdir(input_dir) if f.endswith('.pdf')]
    print(f"[INFO] Found {len(pdf_files)} PDF files to process.")

    if not pdf_files:
        print("[WARN] No PDF files found in the input directory.")
        return
    
    # Iterate through each PDF file and extract tables
    for pdf_file in pdf_files:

        # Open extract_log.csv file 
        log_file_path = "extract_log.csv"
        log_file = pd.read_csv(log_file_path)
        print(f"[INFO] Loaded log file: {log_file_path}")

        print(f"\n[INFO] Processing file: {pdf_file}, number {pdf_files.index(pdf_file) + 1} of {len(pdf_files)}")
        
        # Skip file if already processed and recreate_flag is 0
        if pdf_file in log_file['input_pdf_name'].values:
            recreate_flag = log_file.loc[log_file['input_pdf_name'] == pdf_file, 'recreate_flag'].values[0]
            if recreate_flag == 0:
                print(f"[INFO] Skipping {pdf_file} (already processed, recreate_flag=0).")
                continue
        else:
            
            # Add new entry to log file with recreate_flag set to 1
            new_log_entry = pd.DataFrame({
                'input_pdf_name': [pdf_file],
                'output_csv_name': [f"{os.path.splitext(pdf_file)[0]}.csv"],
                'error_flag': [0],
                'recreate_flag': [1]
            })
            log_file = pd.concat([log_file, new_log_entry], ignore_index=True)
            print(f"[INFO] Added {pdf_file} to log file.")
        
        # Load the PDF file
        pdf_path = os.path.join(input_dir, pdf_file)
        output_csv_name = f"{os.path.splitext(pdf_file)[0]}.csv"
        output_csv_path = os.path.join(output_dir, output_csv_name)

        try:
            # Extract tables from the PDF file
            csv_text = extract_pdf_tables(pdf_path, output_dir)
            print(f"[INFO] Extracted CSV text for {pdf_file}.")

            # Convert string to file-like object
            buff = StringIO(csv_text)
            
            # Read as DataFrame
            df = pd.read_csv(buff)
            print(f"[INFO] Parsed CSV text into DataFrame for {pdf_file}.")

            # Save DataFrame to CSV
            df.to_csv(output_csv_path, index=False)
            print(f"[SUCCESS] Saved extracted tables to {output_csv_name}.")
            
            # Update the log file with success
            log_file.loc[log_file['input_pdf_name'] == pdf_file, 'error_flag'] = 0
            log_file.loc[log_file['input_pdf_name'] == pdf_file, 'recreate_flag'] = 0
            
        except Exception as e:
            print(f"[ERROR] Error extracting tables from {pdf_file}: {e}")
            # Update the log file with error
            log_file.loc[log_file['input_pdf_name'] == pdf_file, 'error_flag'] = 1
            log_file.loc[log_file['input_pdf_name'] == pdf_file, 'recreate_flag'] = 1

        # Save the updated log file
        log_file.to_csv(log_file_path, index=False)
        print(f"[INFO] Updated log file saved: {log_file_path}")

if __name__ == "__main__":
    main()
