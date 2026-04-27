import os
import glob

def combine_csvs(directory, output_filename, sensor_label):
    # Find all CSV files except the combined one itself
    csv_files = sorted([f for f in glob.glob(os.path.join(directory, "*.csv")) 
                        if os.path.basename(f) != output_filename])
    
    if not csv_files:
        print(f"No CSV files found in {directory} for {sensor_label}")
        return

    print(f"Combining {len(csv_files)} files in {directory} -> {output_filename}...")
    
    output_path = os.path.join(directory, output_filename)
    
    header_written = False
    with open(output_path, 'w', encoding='utf-8') as outfile:
        for i, filepath in enumerate(csv_files):
            with open(filepath, 'r', encoding='utf-8') as infile:
                lines = infile.readlines()
                if not lines:
                    continue
                
                # Write header only from the first file
                if not header_written:
                    outfile.write(lines[0])
                    header_written = True
                
                # Write data lines (skip header for subsequent files)
                outfile.writelines(lines[1:])
                
            if (i + 1) % 50 == 0:
                print(f"  Processed {i + 1} files...")

    print(f"Successfully created {output_path}")

if __name__ == "__main__":
    # Paths relative to the data-loader directory
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    PARENT_DIR = os.path.dirname(BASE_DIR)
    
    BME_DIR = os.path.join(PARENT_DIR, "clickhouse-bme-data")
    SDS_DIR = os.path.join(PARENT_DIR, "clickhouse-sds-data")
    
    combine_csvs(BME_DIR, "combined_bme.csv", "BME280")
    combine_csvs(SDS_DIR, "combined_sds.csv", "SDS011")
