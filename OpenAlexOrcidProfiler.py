import os
import pandas as pd
import glob
import simplejson as json
import concurrent.futures
import requests
import time
from datetime import datetime
import traceback

# Since I'm saving my outputs to a simple excel table, there's a limit to the number of rows I can have
MAX_EXCEL_ROWS = 999999
# Global variable to keep track of the number of ORCID API requests made
# Total allowed = 5000 daily, 5 per second API calls for base tier API users
ORCID_REQUEST_COUNT = 0


def find_json_files(directory):
    # Use glob to locate the paths of all the .json files in the OpenAlex snapshot I have on my D: drive
    return glob.glob(os.path.join(directory, '**', '*.json'), recursive=True)


def safe_get(d, *keys, default=None):
    # Helper function to safely get a value from a nested dictionary
    for key in keys:
        if isinstance(d, dict):
            d = d.get(key, {})
        else:
            return default
    return d or default


def is_leap_year(year):
    # Helper function to check if the year is a leap year
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)


def safe_datetime(year, month, day):
    # Helper function to safely create a datetime object
    try:
        return datetime(year, month, day)
    except ValueError:
        # Adjust day to the last day of the month
        if month == 2:
            day = 29 if is_leap_year(year) else 28
        elif month in [4, 6, 9, 11]:  # Months with 30 days
            day = 30
        else:  # Months with 31 days
            day = 31
        return datetime(year, month, day)


def safe_get_list(d, *keys):
    # Safely get a value from a nested dictionary containing lists
    value = safe_get(d, *keys[:-1])
    if isinstance(value, list) and len(value) > 0:
        return value[0].get(keys[-1], None)
    return None


def extract_orcid_info(orcid):
    global ORCID_REQUEST_COUNT
    # ORCID API has a 5000 per day request limit for non-API, base tier users
    if ORCID_REQUEST_COUNT >= 5000:
        return None, None, None

    # Clean up the ORCID ID, which OpenAlex gives as full URL (https://orcid.org/0000-0002-1825-0097)
    orcid_cleaned = orcid.replace(
        "https://orcid.org/", "")  # remove the URL prefix
    base_url = "https://pub.orcid.org/v3.0/"  # ORCID API base URL
    # Format the ORCID to match the API URL expected by ORCID
    url = base_url + orcid_cleaned + "/record"
    headers = {"Accept": "application/json"}
    response = requests.get(url, headers=headers)

    ORCID_REQUEST_COUNT += 1  # Increment the tally of ORCID API calls

    if response.status_code == 200:  # This is the code for a successful request
        # Extract email, degree status, and LinkedIn URL from the response
        data = response.json()
        email = safe_get_list(data, 'person', 'emails', 'email', 'email')
        educations_group = data.get(
            'activities-summary', {}).get('educations', {}).get('affiliation-group', [])

        degree_statuses = []
        for edu_group in educations_group:
            # For a given researcher, they may have multiple degrees from multiple institutions
            summaries = edu_group.get('summaries', [])
            for summary in summaries:
                # For a given degree, there may be multiple summaries (e.g. one for each institution)
                education = summary.get('education-summary', {})
                degree = safe_get(education, 'role-title')
                institution = safe_get(education, 'organization', 'name')
                end_date_data = safe_get(education, 'end-date')
                end_date = safe_datetime(int(safe_get(end_date_data, 'year', 'value', default="1900")),
                                         int(safe_get(end_date_data,
                                             'month', 'value', default="1")),
                                         int(safe_get(end_date_data, 'day', 'value', default="1")))

                current_date = datetime.now()

                start_date_data = safe_get(education, 'start-date')
                start_year = safe_get(
                    start_date_data, 'year', 'value', default="")

                end_year = safe_get(end_date_data, 'year', 'value', default="")
                # We can parse the degrees in a natural language statement based on the start and end dates
                if not end_date_data or end_date > current_date:
                    degree_status = f"Currently pursuing {degree} at {institution} (Started {start_year})" if degree and institution else None
                else:
                    degree_status = f"Graduated with {degree} from {institution} in {end_year}" if degree and institution else None

                if degree_status:
                    # If we sucessfully parsed the degree status, append it to the list
                    degree_statuses.append(degree_status)
        # Combine the degree statuses into a single string
        combined_degree_status = '; '.join(degree_statuses)
        # Extract LinkedIn URL, which occasionally researchers store on their ORCID
        researcher_urls = safe_get(
            data, 'person', 'researcher-urls', 'researcher-url') or []

        linkedin_url = None
        # You can link a number of URL's on ORCID so we just need to find the LinkedIn one
        for r_url in researcher_urls:
            if r_url.get('url-name') == 'LinkedIn':
                linkedin_url = r_url.get('url', {}).get('value')
                break

        return email, combined_degree_status, linkedin_url
    # Strange error code I've ran into a few times for a select group of ORCID's (not based on API tiers)
    elif response.status_code == 409:
        print(f"ORCID record {orcid} is locked and cannot be accessed.")
        return None, None, None
    else:  # Just catch all other errors
        print(
            f"Failed to fetch details for ORCID: {orcid}. Status code:", response.status_code)
        print("Response content:", response.text)
        return None, None, None


def extract_info(input_file, relevant_subject):
    # Call global ORCID request count (keeps track of total requests to respect API limits)
    global ORCID_REQUEST_COUNT
    # Initialize lists to store data
    display_names = []
    orcids = []
    x_concepts_display_names = []
    h_indices = []
    cited_by_counts_list = []
    last_known_universities = []
    emails = []
    degrees_status_list = []
    linkedins = []
    # Depth of OpenAlex tags we want to filter above (less broad topics)
    LEVEL_THRESHOLD = 1
    # Open the JSON file
    with open(input_file, 'r') as infile:
        for line in infile:
            try:
                entry = json.loads(line.strip())
            except json.JSONDecodeError:
                print(f"Error decoding JSON in file {input_file}")
                continue
            # Extract data from JSON
            display_name = entry['display_name']
            orcid = entry.get('orcid', None)
            cited_by_count = entry.get('cited_by_count', None)
            concepts = entry.get('x_concepts', [])
            counts_by_year = entry.get('counts_by_year', [])

            # Filter critieria 1
            # Must have a valid Full name, ORCID, Cited By Count between 5 and 1000, and expertise in relevant subject
            # [Idea]: This is a great spot to build a UI around for Journals to find applicable peer reviewers
            if not (display_name and orcid and (5 < cited_by_count < 1000)
                    and relevant_subject in [concept['display_name'] for concept in concepts]):
                continue
            # Prep for Filter 2
            current_year = datetime.now().year
            # Must have published within the last 5 years
            recent_years = [current_year - i for i in range(5)]
            # Check that the user has a publication within the last 5 years (recent_years array)
            recent_publication = any(
                year_count['year'] in recent_years and year_count['works_count'] >= 1 for year_count in counts_by_year)
            # Filter critieria 2
            # Must have published within the last 5 years
            if not recent_publication:
                continue
            # Extract last known university safely
            # If last_known_institution key is missing, it will be 'None'
            # If last_known_institution doesn't have a display_name  it'll be 'None' still
            # If both keys exist it will have the value: entry['last_known_institution']['display_name']
            last_known_institution = entry.get(
                'last_known_institution', {})
            last_known_university = last_known_institution.get(
                'display_name', None) if last_known_institution else None
            # Extract OpenAlex tags above the concept level threshold and store them
            filtered_concepts = [
                concept for concept in concepts if concept['level'] >= LEVEL_THRESHOLD]
            # Sort them by their % match with the researcher
            sorted_concepts = sorted(
                filtered_concepts, key=lambda x: x['score'], reverse=True)
            # Make sure to only grab the unique concept names
            unique_concept_names = [concept['display_name']
                                    for concept in sorted_concepts]
            concepts_str = ', '.join(unique_concept_names)
            # Extract H-Index from OpenAlex
            h_index = entry.get('summary_stats', {}).get('h_index', None)
            # Append all the data to their respective lists
            display_names.append(display_name)
            orcids.append(orcid)
            x_concepts_display_names.append(concepts_str)
            h_indices.append(h_index)
            cited_by_counts_list.append(cited_by_count)
            last_known_universities.append(last_known_university)
            # Ping the ORCID API for the researcher's email, degree status, and LinkedIn URL
            if orcid:
                email, degree_status, linkedin = extract_orcid_info(orcid)
                # Pause for 0.2 seconds to respect the API RPS limit
                time.sleep(0.2)
            else:
                email, degree_status, linkedin = None, None, None
            # Append remaining data to their respective lists
            emails.append(email)
            degrees_status_list.append(degree_status)
            linkedins.append(linkedin)
    # Create a new dataframe with all the data once we've read every line of the .json files
    df_new = pd.DataFrame({
        'Display Name': display_names,
        'ORCID': orcids,
        'Concepts': x_concepts_display_names,
        'H-Index': h_indices,
        'Cited By Count': cited_by_counts_list,
        'Last Known University': last_known_universities,
        'Degree and Institution': degrees_status_list,
        'Email': emails,
        'LinkedIn': linkedins
    })

    return df_new


# 'D:\\openalex-snapshot\\data\\authors'
# 'C:\\Users\\tydio\\Documents\\OpenAlex\\TestData'):

# Main Driving function
def extract_and_save(relevant_subject, directory='D:\\openalex-snapshot\\data\\authors'):
    # OpenAlex data snapshot is saved on my D: drive so I locate all the .json files in the snapshot first
    all_json_files = find_json_files(directory)
    print(f"Found {len(all_json_files)} JSON files.")
    dfs = []
    # Then submit each .json file to the extract_info function to extract the relevant data
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(extract_info, file_path, relevant_subject)
                   for file_path in all_json_files]
        for future in concurrent.futures.as_completed(futures):
            # Try to append the data to the list of dataframes
            try:
                dfs.append(future.result())
            except Exception as e:
                print(f"Exception occurred: {str(e)}")
                traceback.print_exc()
    if not dfs:  # Check if dfs is empty
        print("No data to save. Exiting.")
        return
    # Concatenate all the dataframes into one
    final_df = pd.concat(dfs, ignore_index=True)
    # May need multiple files if the dataframe is too large, as by MAX_EXCEL_ROWS
    file_counter = 0
    # Do the saving to .xlsx, which is the local format I wanted
    while not final_df.empty:
        # Trim the dataframe to the max number of rows allowed
        current_df = final_df.head(MAX_EXCEL_ROWS)
        final_df = final_df.tail(final_df.shape[0] - MAX_EXCEL_ROWS)
        # Save the dataframe to an excel file
        # make sure spaces don't mess it up
        filename = f"./{relevant_subject.replace(' ', '_')}_{file_counter}.xlsx"
        current_df.to_excel(filename, index=False, engine='openpyxl')

        # Increment the file counter because if any data remains we'll need to go back through to the next excel file save
        file_counter += 1
    print(f"Total ORCID requests made: {ORCID_REQUEST_COUNT}")
    print()


# Usage
# As many calls here as we want for concept tags
extract_and_save("Catalysis")
extract_and_save("Biomarkers")
extract_and_save("Ebola virus")
extract_and_save("Virology")
