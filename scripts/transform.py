import pandas as pd

def transform_spotify(df):
    df = df.drop_duplicates()
    df = df.dropna(subset=['artists', 'album_name', 'track_name'])
    df['duration_min'] = (df['duration_ms'] / 60000).round(2)
    df = df.drop(columns=['duration_ms'])
    df['artists'] = df['artists'].str.strip()
    df['track_name'] = df['track_name'].str.strip()
    df['track_genre'] = df['track_genre'].str.strip()
    df['explicit'] = df['explicit'].astype(int)
    print(f"Spotify transformado: {df.shape[0]} filas")
    return df

def transform_grammy(df):
    df = df.drop(columns=['img', 'published_at', 'updated_at', 'workers'], errors='ignore')
    df['artist'] = df['artist'].fillna('Unknown')
    df['nominee'] = df['nominee'].fillna('Unknown')
    df['artist'] = df['artist'].str.strip()
    df['nominee'] = df['nominee'].str.strip()
    df['category'] = df['category'].str.strip()
    df['winner'] = df['winner'].astype(int)
    print(f"Grammy transformado: {df.shape[0]} filas")
    return df

def merge_datasets(spotify_df, grammy_df):
    grammy_df['artist_lower'] = grammy_df['artist'].str.lower()
    spotify_df['artist_lower'] = spotify_df['artists'].str.lower()
    merged = spotify_df.merge(grammy_df, on='artist_lower', how='left')
    merged = merged.drop(columns=['artist_lower'])
    merged['winner'] = merged['winner'].fillna(0).astype(int)
    merged['grammy_nominated'] = merged['nominee'].notna().astype(int)
    print(f"Dataset unido: {merged.shape[0]} filas")
    return merged