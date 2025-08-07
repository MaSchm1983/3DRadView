import asyncio
import aiohttp
import aiofiles
import os
import re
import json
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

# ---- CONFIG ----
HOST_URL = 'https://opendata.dwd.de/weather/radar/sites/pz/'
TARGET_DIR = os.path.join(os.path.dirname(__file__), '3D_RAD_DATA')
INDEX_FILE_PATH = os.path.join(TARGET_DIR, 'current_files.json')

HD5_FILENAME_REGEX = re.compile(r'_(\d+)-(\d{14})-.*?([a-zA-Z]{3})-hd5$')

# ---- UTILS ----
def extract_info_from_filename(filename):
    m = HD5_FILENAME_REGEX.search(filename)
    if not m:
        return None
    return {
        "RadID": int(m.group(1)),
        "timestamp": m.group(2),
        "site": m.group(3)
    }

async def fetch_directory_listing(session, url):
    try:
        async with session.get(url) as resp:
            html = await resp.text()
        soup = BeautifulSoup(html, 'html.parser')
        links = [a.get('href') for a in soup.find_all('a') if a.get('href') and a.get('href') != '../']
        return links
    except Exception as e:
        print(f'❌ Failed to fetch {url}: {e}')
        return []

async def download_file(session, file_url, local_path, retries=3):
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    for attempt in range(retries):
        try:
            async with session.get(file_url) as resp:
                if resp.status != 200:
                    raise Exception(f"HTTP {resp.status}")
                async with aiofiles.open(local_path, 'wb') as f:
                    while True:
                        chunk = await resp.content.read(1024*32)
                        if not chunk:
                            break
                        await f.write(chunk)
            print(f"⬇️  Downloaded: {os.path.basename(local_path)}")
            return True
        except Exception as e:
            if attempt < retries - 1:
                print(f"🔁 Retrying {file_url} ({retries - attempt - 1} attempts left): {e}")
                await asyncio.sleep(1)
            else:
                print(f"❌ Failed to download {file_url}: {e}")
    return False

async def sync_directory(session, url, local_dir, concurrency=12, folder_conc=17):
    links = await fetch_directory_listing(session, url)
    dirs = [link for link in links if link.endswith('/')]
    files = [link for link in links if not link.endswith('/')]

    hd5_files = []
    for link in files:
        file_name = os.path.basename(link)
        extracted = extract_info_from_filename(file_name)
        if extracted:
            hd5_files.append({'file_name': file_name, 'link': link, 'extracted': extracted})

    found_files = set()
    semaphore = asyncio.Semaphore(concurrency)

    async def download_task(file_name, link):
        local_path = os.path.join(local_dir, file_name)
        file_url = urljoin(url, link)
        rel_path = os.path.relpath(local_path, TARGET_DIR)
        found_files.add(rel_path)
        if not os.path.exists(local_path):
            async with semaphore:
                await download_file(session, file_url, local_path)
        else:
            #print(f"✔️  Exists: {file_name}")
            pass

    tasks = [download_task(f['file_name'], f['link']) for f in hd5_files]
    await asyncio.gather(*tasks)

    # Subfolder recursion, limited by folder_conc
    folder_semaphore = asyncio.Semaphore(folder_conc)
    async def folder_task(subdir):
        new_url = urljoin(url, subdir)
        new_local = os.path.join(local_dir, subdir)
        async with folder_semaphore:
            found = await sync_directory(session, new_url, new_local, concurrency, folder_conc)
            found_files.update(found)

    folder_tasks = [folder_task(subdir) for subdir in dirs]
    await asyncio.gather(*folder_tasks)
    return found_files

async def get_all_files(root):
    out = []
    for dirpath, dirnames, filenames in os.walk(root):
        for fn in filenames:
            full = os.path.join(dirpath, fn)
            rel = os.path.relpath(full, TARGET_DIR)
            out.append(rel)
    return out

async def clean_local_files(remote_files):
    local_files = await get_all_files(TARGET_DIR)
    for file in local_files:
        if file not in remote_files:
            full_path = os.path.join(TARGET_DIR, file)
            try:
                os.remove(full_path)
                print(f"🗑️  Deleted: {file}")
            except Exception as e:
                print(f"❌ Failed to delete {file}: {e}")

async def build_full_index_from_local():
    index = []
    for dirpath, _, filenames in os.walk(TARGET_DIR):
        for fn in filenames:
            if fn.endswith('-hd5'):
                full_path = os.path.join(dirpath, fn)
                rel_path = os.path.relpath(full_path, TARGET_DIR)
                meta = extract_info_from_filename(fn)
                if meta:
                    index.append({"path": rel_path, **meta})
    return index

# ---- MAIN ----
async def start_sync():
    print(f"🚀 Starting sync from {HOST_URL}")
    os.makedirs(TARGET_DIR, exist_ok=True)
    async with aiohttp.ClientSession() as session:
        remote_files = await sync_directory(session, HOST_URL, TARGET_DIR, 12, 17)
        await clean_local_files(remote_files)
        file_index = await build_full_index_from_local()
        async with aiofiles.open(INDEX_FILE_PATH, 'w') as f:
            await f.write(json.dumps(file_index, indent=2, ensure_ascii=False))
        print(f"✅ Sync complete. Index saved to {INDEX_FILE_PATH}")

if __name__ == '__main__':
    asyncio.run(start_sync())