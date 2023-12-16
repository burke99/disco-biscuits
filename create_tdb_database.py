import pandas as pd
import json
import requests
from pandas import json_normalize


def load_json_from_url(url):
    try:
        # Send a GET request to the URL
        response = requests.get(url)
        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse the JSON content
            data = response.json()
            return data
        else:
            print(f"Failed to fetch {url}. Status Code: {response.status_code}")
            return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

url = "https://api.discobiscuits.net/api/shows"  # Replace with your JSON URL

data = load_json_from_url(url)

# Flatten the JSON data and handle tracks separately
flattened_records = []


for record in data:
    # Flatten the main record
    flattened_data = json_normalize(record)

    #rename slug column to show_slug
    flattened_data['show_slug'] = flattened_data.pop('slug')

    # Filter records with non-null song_id
    flattened_records = [record for record in flattened_records if record.get('song_id') is not None]

    # Flatten the 'tracks' column
    tracks = json_normalize(record, 'tracks', sep='_')
    # Combine the main record and tracks data
    combined_data = pd.concat([flattened_data, tracks], axis=1)
    flattened_records.append(combined_data)

# Concatenate all records into a single DataFrame
df = pd.concat(flattened_records, ignore_index=False)
df = df.reset_index()


# Forward-fill missing values in the top-level fields
df.ffill(inplace=True)


# Define a custom function to convert 'set' and 'position' to numeric values
def set_position_to_numeric(set_value):
    if set_value.startswith('S'):
        return int(set_value[1:])
    elif set_value.startswith('E'):
        return int(set_value[1:]) + 100  # To distinguish E sets from S sets
    else:
        return 0

# Apply the custom function to create 'song_order'
df['song_order_in_show'] = df.apply(lambda row: set_position_to_numeric(row['set']) * 100 + row['position'], axis=1)

# Define a custom function to convert 'set' and 'position' to numeric values
def set_position_to_numeric(set_value):
    if set_value.startswith('S'):
        return int(set_value[1:])
    elif set_value.startswith('E'):
        return int(set_value[1:]) + 100  # To distinguish E sets from S sets
    else:
        return 0

# Apply the custom function to create 'song_order'
df['song_order'] = df.apply(lambda row: set_position_to_numeric(row['set']) * 100 + row['position'], axis=1)

# Sort DataFrame by 'set' and 'position'
df.sort_values(by=['date','set', 'position'], inplace=True)

# Create new columns for song before and after within the same set
df['song_slug_before'] = df['song_slug'].shift(1)
df['song_title_before'] = df['song_title'].shift(1)
df['song_slug_after'] = df['song_slug'].shift(-1)
df['song_title_after'] = df['song_title'].shift(-1)

# Filter out rows where 'set' changes or where 'position' is the last in a set
df['song_slug_before'] = df.apply(lambda row: row['song_slug_before'] if row['set'] == row['set'] and row['position'] != 1 else None, axis=1)
df['song_title_before'] = df.apply(lambda row: row['song_title_before'] if row['set'] == row['set'] and row['position'] != 1 else None, axis=1)
df['song_slug_after'] = df.apply(lambda row: row['song_slug_after'] if row['set'] == row['set'] and row['position'] != df[df['set'] == row['set']]['position'].max() else None, axis=1)
df['song_title_after'] = df.apply(lambda row: row['song_title_after'] if row['set'] == row['set'] and row['position'] != df[df['set'] == row['set']]['position'].max() else None, axis=1)

# Create 'show_order' based on the ascending order of show dates
df['show_order'] = df.sort_values(by=['show_slug', 'date']).groupby('show_slug').ngroup() + 1

#create 'last_show_order' and 'last_show_date' to find the last time the song was played
df['last_show_order'] = df.sort_values(by=['song_slug', 'date']).groupby('song_slug')['show_order'].apply(lambda x: x.shift(1) if (x.shift(1) != x).any() else np.nan)
df['last_show_date'] = df.sort_values(by=['song_slug', 'date']).groupby('song_slug')['date'].apply(lambda x: x.shift(1) if (x.shift(1) != x).any() else np.nan)


# Save the DataFrame to a CSV file
csv_file_path = 'disco_biscuits_database.csv'
df.to_csv(csv_file_path, index=False)

print(f"Flattening complete. CSV file saved at: {csv_file_path}")
