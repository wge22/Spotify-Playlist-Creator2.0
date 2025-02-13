import requests
import datetime  
import pandas as pd
import time
import webbrowser
import os
from urllib.parse import urlencode, urlparse, parse_qs

CLIENT_ID = "4b2f6d09cdeb423492bda923004cc9a2"
CLIENT_SECRET = "d077cb28022d4a1c95afc7b85ca4fa86"
REDIRECT_URI = "http://localhost/"
API_BASE_URL = "https://api.spotify.com/v1/"
TOKEN_URL = "https://accounts.spotify.com/api/token"
AUTH_URL = "https://accounts.spotify.com/authorize"
SCOPE = "user-top-read"

class SpotifyAuth:
    def __init__(self):
        self.access_token = None
        self.refresh_token = None
        self.token_expiry = None
        self.load_token()
        if not self.access_token or datetime.datetime.now() >= self.token_expiry:
            self.get_auth_code()
            self.exchange_code_for_token()
        else:
            print("Token is up to date")

    def get_auth_code(self):
        params = {
            "client_id": CLIENT_ID,
            "response_type": "code",
            "redirect_uri": REDIRECT_URI,
            "scope": SCOPE,
        }
        auth_url = f"{AUTH_URL}?{urlencode(params)}"
        print(f"Please log in: {auth_url}")
        webbrowser.open(auth_url)
        self.auth_code = input("Enter the URL you were redirected to: ")
        self.auth_code = parse_qs(urlparse(self.auth_code).query).get("code", [None])[0]

    def exchange_code_for_token(self):
        data = {
            "grant_type": "authorization_code",
            "code": self.auth_code,
            "redirect_uri": REDIRECT_URI,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        }
        response = requests.post(TOKEN_URL, data=data).json()
        self.access_token = response.get("access_token")
        self.refresh_token = response.get("refresh_token")
        self.token_expiry = datetime.datetime.now() + datetime.timedelta(seconds=response.get("expires_in", 3600))
        self.save_token()

    def refresh_access_token(self):
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        }
        response = requests.post(TOKEN_URL, data=data).json()
        self.access_token = response.get("access_token")
        self.token_expiry = datetime.datetime.now() + datetime.timedelta(seconds=response.get("expires_in", 3600))
        self.save_token()

    def save_token(self):
        with open("token.txt", "w") as file:
            file.write(f"{self.access_token},{self.refresh_token},{self.token_expiry}")

    def load_token(self):
        try:
            with open("token.txt", "r") as file:
                data = file.read().split(",")
                self.access_token, self.token_expiry = data[0], datetime.datetime.fromisoformat(data[1])
        except (FileNotFoundError, ValueError):
            print("Token file not found or invalid, fetching new token...")

    def get_token(self):
        if datetime.datetime.now() >= self.token_expiry:
            self.refresh_access_token()
        return self.access_token

spotify_auth = SpotifyAuth()

def fetch_spotify_api(endpoint, method='GET', body=None):
    headers = {"Authorization": f"Bearer {spotify_auth.get_token()}"}
    response = requests.request(method, API_BASE_URL + endpoint, headers=headers, json=body)
    return response.json()

def get_song_id(track_name, artist_name):
    query = f'track:"{track_name}" artist:"{artist_name}"'
    endpoint = f"search?{urlencode({'q': query, 'type': 'track'})}"
    response = fetch_spotify_api(endpoint)
    tracks = response.get("tracks", {}).get("items", [])
    return tracks[0]["id"] if tracks else None

def get_audio_features(track_ids):
    if not track_ids:
        return []
    ids_string = ",".join(track_ids)
    endpoint = f"audio-features?ids={ids_string}"
    response = fetch_spotify_api(endpoint)

    # Debugging print to check API response
    print("Audio Features API Response:", response)

    return response.get("audio_features", [])

def process_csv(file_path, output_file="processed_songs.csv"):
    # Read CSV with correct delimiter and handle quotes properly
    df = pd.read_csv(file_path, delimiter=";", quotechar='"')

    # Ensure required columns exist
    required_columns = {"Artist", "Album", "Track"}
    if not required_columns.issubset(df.columns):
        raise ValueError(f"CSV is missing required columns: {required_columns - set(df.columns)}")

    # Count occurrences of each song
    df["Play Count"] = df.groupby(["Artist", "Album", "Track"])["Track"].transform("count")

    # Keep only songs that have been played at least 20 times
    df = df[df["Play Count"] >= 4]

    # Drop duplicate entries, keeping only one unique song entry
    df = df.drop_duplicates(subset=["Artist", "Album", "Track"])

    # Load existing processed file to continue progress
    if os.path.exists(output_file):
        processed_df = pd.read_csv(output_file)
        df = df.merge(processed_df[["Artist", "Album", "Track", "Spotify_ID"]], on=["Artist", "Album", "Track"], how="left")
    else:
        df["Spotify_ID"] = None

    # Find missing Spotify IDs
    missing_rows = df[df["Spotify_ID"].isna()].copy()

    for index, row in missing_rows.iterrows():
        spotify_id = get_song_id(row["Track"], row["Artist"])
        df.at[index, "Spotify_ID"] = spotify_id

        # Save after each API call
        df.to_csv(output_file, index=False)
        print(f"Updated {row['Track']} - {row['Artist']} with Spotify ID {spotify_id}")
        time.sleep(1)  # Avoid rate limiting

    # Remove rows where no Spotify ID was found
    track_ids = df["Spotify_ID"].dropna().tolist()

    # Fetch audio features in batches of 100
    audio_features = []
    for i in range(0, len(track_ids), 100):
        print(f"Batch {i // 100 + 1}: Fetching songs {i} - {i + 100}...")
        batch = track_ids[i:i+100]
        audio_features.extend(get_audio_features(batch))
        time.sleep(1)  # Avoid rate limiting

    # Convert API response to DataFrame
    features_df = pd.DataFrame(audio_features)

    # Filter out rows where 'id' is None
    valid_features_df = features_df.dropna(subset=["id"])

    # Store missing tracks separately
    missing_tracks = features_df[features_df["id"].isna()]
    if not missing_tracks.empty:
        missing_tracks.to_csv("missing_tracks.csv", index=False)
        print(f"Stored {len(missing_tracks)} missing tracks in 'missing_tracks.csv' for manual addition.")

    # Merge only valid data
    if valid_features_df.empty:
        print("Error: No valid audio features found after filtering. Check API response.")
    else:
        final_df = df.merge(valid_features_df, left_on="Spotify_ID", right_on="id", how="left")
        final_df.to_csv(output_file, index=False)
        print(f"Processed data saved to {output_file} with {len(final_df)} songs.")

# Run the processing
process_csv(r"C:\Users\willi\OneDrive\Desktop\Projects\lastfmstats-Wge22 (1).csv")
