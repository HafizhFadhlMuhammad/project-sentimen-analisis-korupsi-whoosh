import os
import time
import csv

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv
import pandas as pd

load_dotenv()
API_KEY = os.getenv("YOUTUBE_API_KEY")

if API_KEY is None:
    raise ValueError(
        "YOUTUBE_API_KEY tidak ditemukan.\n"
        "Pastikan kamu sudah membuat file .env dan mengisinya seperti:\n"
        "YOUTUBE_API_KEY=API_KEY_KAMU"
    )

youtube = build("youtube", "v3", developerKey=API_KEY)

video_ids = [
    '1_Xrj0mb7K4',
    'ScKSfYjttk0',
    'VBwXF2JT6NQ',
    '9L3NRHO743w',
    'tyAGcaj7Wa0',
    'bb-XWIaXFGQ',
    'c2U0v-OnsaE',
    'vI9ZqpUl1YQ',
    'xHaMtz73wQc',
    'Su2ZvF0EJDI'
]

MAX_COMMENTS_PER_VIDEO = 200
OUTPUT_FILENAME = "../data/raw_dataset_whoosh.csv"

def get_video_title(video_id: str) -> str:
    try:
        response = youtube.videos().list(
            part="snippet",
            id=video_id
        ).execute()

        items = response.get("items", [])
        if items:
            return items[0]["snippet"].get("title", "N/A")
        else:
            return "N/A"

    except HttpError as e:
        print(f"  [ERROR] Gagal mengambil judul untuk video {video_id}: {e}")
    except Exception as e:
        print(f"  [ERROR] Terjadi kesalahan tak terduga saat mengambil judul: {e}")

    return "N/A"


def get_video_comments(video_id: str, video_title: str, max_comments: int):
    comments = []
    next_page_token = None

    if max_comments <= 0:
        return comments

    print(f"  -> Mengambil komentar (maks: {max_comments})...")

    while len(comments) < max_comments:
        request_count = min(100, max_comments - len(comments))

        try:
            response = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=request_count,
                textFormat="plainText",
                pageToken=next_page_token
            ).execute()
        except HttpError as e:
            err_str = str(e)
            if "commentsDisabled" in err_str:
                print(f"  [INFO] Komentar dimatikan untuk video {video_id}.")
            elif "quotaExceeded" in err_str:
                print("  [ERROR] Kuota API habis. Hentikan script.")
                raise
            else:
                print(f"  [ERROR] Gagal mengambil komentar untuk video {video_id}: {e}")
            break
        except Exception as e:
            print(f"  [ERROR] Terjadi kesalahan tak terduga: {e}")
            break

        items = response.get("items", [])
        if not items:
            break

        for item in items:
            if len(comments) >= max_comments:
                break

            try:
                top = item["snippet"]["topLevelComment"]
                snippet = top["snippet"]

                comment_id = top.get("id", "")
                comment_text = snippet.get("textOriginal", "")
                author = snippet.get("authorDisplayName", "Unknown")
                like_count = snippet.get("likeCount", 0)
                published_at = snippet.get("publishedAt", "")

                comments.append({
                    "video_id": video_id,
                    "video_title": video_title,
                    "comment_id": comment_id,
                    "author": author,
                    "comment": comment_text,
                    "likes": like_count,
                    "published_at": published_at
                })
            except KeyError:
                print("  [WARNING] Melewatkan 1 komentar dengan format tidak terduga.")
                continue

        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break

    print(f"  -> Selesai. Mendapatkan {len(comments)} komentar dari video ini.")
    return comments[:max_comments]


def save_comments_to_csv(comments, filename: str):
    fieldnames = [
        "video_id",
        "video_title",
        "comment_id",
        "author",
        "comment",
        "likes",
        "published_at"
    ]

    if not comments:
        print("[INFO] Tidak ada komentar untuk disimpan.")
        return

    os.makedirs(os.path.dirname(filename) or ".", exist_ok=True)

    with open(filename, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(comments)

    print(f"[INFO] Berhasil menyimpan {len(comments)} komentar ke '{filename}'.")


def scrape_comments_from_videos(video_ids, output_filename: str):
    all_comments = []

    if not video_ids:
        print("[ERROR] Daftar video_ids kosong.")
        return []

    print(f"Memulai scraping untuk {len(video_ids)} video.")
    print(f"Target: Maksimal {MAX_COMMENTS_PER_VIDEO} komentar per video.")
    print("-" * 50)

    for i, vid in enumerate(video_ids):
        print(f"[{i+1}/{len(video_ids)}] Video ID: {vid}")

        title = get_video_title(vid)
        print(f"  -> Judul: {title}")

        comments = get_video_comments(vid, title, MAX_COMMENTS_PER_VIDEO)
        all_comments.extend(comments)

        time.sleep(0.5)

    print("-" * 50)
    print(f"Scraping selesai. Total komentar terkumpul: {len(all_comments)}")

    save_comments_to_csv(all_comments, output_filename)

    return all_comments

if __name__ == "__main__":
    comments = scrape_comments_from_videos(video_ids, OUTPUT_FILENAME)

    if comments:
        df = pd.DataFrame(comments)
        print("\nRingkasan DataFrame:")
        print(df.info())
        print("\nData Teratas (head):")
        print(df.head())
    else:
        print("\nTidak ada komentar yang diambil. DataFrame tidak dibuat.")