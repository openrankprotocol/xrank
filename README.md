# XRank - Trust Score Generator

## Overview

XRank is a trust score generation system that calculates trust relationships based on Twitter/X interactions. It fetches user data, processes interactions (follows, mentions, replies, retweets, quotes), and generates trust matrices for use with EigenTrust algorithms.

## Project Structure

```
xrank/
├── config.toml                 # Configuration file
├── requirements.txt            # Python dependencies
├── fetch_followings.py         # Fetch seed users' followings
├── fetch_extended_followings.py # Fetch followings within master list
├── fetch_interactions.py       # Fetch posts/tweets from users
├── fetch_usernames.py          # Fetch usernames for scored user IDs
├── generate_trust.py           # Generate trust score matrices
├── generate_seed.py            # Generate seed CSV files for EigenTrust
├── process_scores.py           # Process EigenTrust output scores
├── raw/                        # Raw data from API
│   ├── {seed_graph}_followings.json
│   ├── {seed_graph}_extended_followings.json
│   ├── {seed_graph}_{start_id}_{end_id}.json
│   └── {seed_graph}_usernames.csv
├── trust/                      # Trust matrices output
│   └── {seed_graph}.csv
├── seed/                       # Seed files for EigenTrust
│   └── {seed_graph}.csv
├── scores/                     # EigenTrust output scores
│   └── {seed_graph}.csv
└── output/                     # Final processed output
    └── {seed_graph}.csv
```

## Workflow

### 1. Configure Seed Users

Edit `config.toml` to define your seed graph(s):

```toml
[seed_graph]
optimism = [
    "1339694846",  # user_id_1
    "1379415847",  # user_id_2
    # ... more seed user IDs
]
```

### 2. Fetch Followings

Fetch the followings of all seed users to create a master list:

```bash
python fetch_followings.py
```

This creates `raw/{seed_graph}_followings.json` containing:
- Seed users and their profile data
- All users followed by seed users (deduplicated)

### 3. Fetch Extended Followings

Fetch follow relationships within the master list:

```bash
python fetch_extended_followings.py
```

This creates `raw/{seed_graph}_extended_followings.json` containing follow relationships where both users are in the master list.

### 4. Fetch Interactions

Fetch posts, replies, retweets, and quotes from all users in the master list:

```bash
python fetch_interactions.py
```

This creates multiple files `raw/{seed_graph}_{start_id}_{end_id}.json` containing:
- User posts and replies
- Interaction data (mentions, retweets, quotes)

### 5. Generate Trust Scores

Generate trust matrices from the collected data:

```bash
python generate_trust.py
```

This creates `trust/{seed_graph}.csv` with format:
- Header: `i,j,v`
- `i` = source user ID
- `j` = target user ID
- `v` = aggregated trust score

### 6. Generate Seed Files

Generate seed CSV files for EigenTrust:

```bash
python generate_seed.py
```

This creates `seed/{seed_graph}.csv` with normalized seed weights.

### 7. Run EigenTrust (External)

Run the EigenTrust algorithm with the generated trust and seed files. Output should be saved to `scores/{seed_graph}.csv`.

### 8. Process Scores

Convert EigenTrust scores to human-readable format:

```bash
python process_scores.py
```

This creates `output/{seed_graph}.csv` with:
- Usernames mapped from user IDs
- Scores transformed via log2 and normalized to 0.0-1.0 range

## Configuration

### config.toml

```toml
[data]
# Number of days to go back for fetching posts
days_back = 365

[seed_graph]
# Named seed graphs with lists of user IDs
optimism = ["1339694846", "1379415847", ...]

[output]
# Directory for raw data output
raw_data_dir = "./raw"

[rate_limiting]
# Maximum requests per second
requests_per_second = 1000
# Maximum parallel requests
max_parallel_requests = 20

[trust_weights]
# Trust weights for different interaction types
follow = 30
mention = 30
reply = 20
retweet = 50
quote = 40
```

## Trust Weights

Trust scores are calculated based on interaction types with configurable weights:

| Interaction | Default Weight | Description |
|-------------|----------------|-------------|
| Follow      | 30             | User A follows user B |
| Mention     | 30             | User A mentions user B in a post |
| Reply       | 20             | User A replies to user B's post |
| Retweet     | 50             | User A retweets user B's post |
| Quote       | 40             | User A quote-tweets user B's post |

## Output Format

### Trust Matrix (`trust/{seed_graph}.csv`)

```csv
i,j,v
123456789,987654321,50
123456789,111222333,30
```

- `i`: Source user ID
- `j`: Target user ID  
- `v`: Aggregated trust score (sum of all interaction weights)

### Seed File (`seed/{seed_graph}.csv`)

```csv
i,v
123456789,0.25
987654321,0.25
111222333,0.25
444555666,0.25
```

- `i`: Seed user ID
- `v`: Normalized weight (sums to 1.0)

### Final Output (`output/{seed_graph}.csv`)

```csv
username,score
alice,0.95
bob,0.87
charlie,0.72
```

## Notes

- All usernames are normalized (lowercase, @ symbol removed)
- Self-loops (i == j) are excluded from the trust matrix
- Duplicate interactions across data files are deduplicated
- The script handles missing files gracefully and continues processing
- API keys should be stored in a `.env` file (not committed to version control)

## Requirements

Install dependencies:

```bash
pip install -r requirements.txt
```

Required packages:
- `toml` - Configuration file parsing
- `python-dotenv` - Environment variable loading

## Environment Variables

Create a `.env` file with your API credentials:

```
API_KEY=your_api_key_here
```
