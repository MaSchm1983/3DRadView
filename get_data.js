const https = require('https');
const fs = require('fs');
const path = require('path');
const cheerio = require('cheerio');
const pLimit = require('p-limit');
const { promisify } = require('util');
const fsPromises = fs.promises;

const hostURL = 'https://opendata.dwd.de/weather/radar/sites/pz/';
const targetDir = path.join(__dirname, '3D_RAD_DATA');
const indexFilePath = path.join(targetDir, 'current_files.json');

const HD5_FILENAME_REGEX = /_(\d+)-(\d{14})-.*?([a-zA-Z]{3})-hd5$/;
let fileIndex = [];

if (!fs.existsSync(targetDir)) {
    fs.mkdirSync(targetDir, { recursive: true });
}

// --- Helpers ---
function extractInfoFromFilename(fileName) {
    const match = fileName.match(HD5_FILENAME_REGEX);
    if (!match) return null;
    return {
        RadID: parseInt(match[1], 10),
        timestamp: match[2],
        site: match[3]
    };
}

async function fetchDirectoryListing(url) {
    return new Promise(resolve => {
        https.get(url, res => {
            let html = '';
            res.on('data', chunk => html += chunk);
            res.on('end', () => {
                const $ = cheerio.load(html);
                const links = [];
                $('a').each((_, elem) => {
                    const href = $(elem).attr('href');
                    if (href && href !== '../') links.push(href);
                });
                resolve(links);
            });
        }).on('error', err => {
            console.error(`❌ Failed to fetch ${url}:`, err.message);
            resolve([]);
        });
    });
}

async function downloadFile(fileUrl, localPath, retries = 3) {
    const dir = path.dirname(localPath);
    await fsPromises.mkdir(dir, { recursive: true });

    return new Promise((resolve, reject) => {
        const tryDownload = () => {
            const fileStream = fs.createWriteStream(localPath);
            https.get(fileUrl, res => {
                if (res.statusCode !== 200) {
                    fileStream.close();
                    fs.unlink(localPath, () => {});
                    if (retries > 0) {
                        console.log(`🔁 Retrying ${fileUrl} (${retries} attempts left)`);
                        retries--;
                        tryDownload();
                    } else {
                        reject(new Error(`❌ Failed to download ${fileUrl} (status ${res.statusCode})`));
                    }
                    return;
                }
                res.pipe(fileStream);
                fileStream.on('finish', () => {
                    fileStream.close();
                    console.log(`⬇️  Downloaded: ${path.basename(localPath)}`);
                    const meta = extractInfoFromFilename(path.basename(localPath));
                    if (meta) {
                        fileIndex.push({ path: path.relative(targetDir, localPath), ...meta });
                    }
                    resolve();
                });
            }).on('error', err => {
                fileStream.close();
                fs.unlink(localPath, () => {});
                if (retries > 0) {
                    console.log(`🔁 Retrying ${fileUrl} (${retries} attempts left): ${err.message}`);
                    retries--;
                    tryDownload();
                } else {
                    reject(err);
                }
            });
        };
        tryDownload();
    });
}

async function syncDirectory(url, localDir, concurrency = 12, folderConcurrency = 17) {
    const links = await fetchDirectoryListing(url);
    const dirs = links.filter(link => link.endsWith('/'));
    const files = links.filter(link => !link.endsWith('/'));

    const hd5Files = files
        .map(link => {
            const fileName = path.basename(link);
            const extracted = extractInfoFromFilename(fileName);
            return extracted ? { fileName, extracted, link } : null;
        })
        .filter(Boolean);

    const foundFiles = new Set();
    const downloadLimiter = pLimit(concurrency);

    const downloadTasks = hd5Files.map(({ fileName, link }) => {
        const localPath = path.join(localDir, fileName);
        const fileUrl = new URL(link, url).href;
        const relPath = path.relative(targetDir, localPath);
        foundFiles.add(relPath);

        if (!fs.existsSync(localPath)) {
            return downloadLimiter(() => downloadFile(fileUrl, localPath));
        }
        return Promise.resolve();
    });

    await Promise.all(downloadTasks);

    // Parallel subfolder sync
    const folderLimiter = pLimit(folderConcurrency);
    const folderTasks = dirs.map(subdir => {
        const newUrl = new URL(subdir, url).href;
        const newLocal = path.join(localDir, subdir);
        return folderLimiter(() =>
            syncDirectory(newUrl, newLocal, concurrency, folderConcurrency)
                .then(set => set.forEach(f => foundFiles.add(f)))
        );
    });

    await Promise.all(folderTasks);
    return foundFiles;
}

async function cleanLocalFiles(remoteFiles) {
    async function getAllFiles(dir) {
        const entries = await fsPromises.readdir(dir, { withFileTypes: true });
        const files = await Promise.all(entries.map(async entry => {
            const fullPath = path.join(dir, entry.name);
            if (entry.isDirectory()) {
                return getAllFiles(fullPath);
            }
            return [path.relative(targetDir, fullPath)];
        }));
        return files.flat();
    }

    const localFiles = await getAllFiles(targetDir);
    for (const file of localFiles) {
        if (!remoteFiles.has(file)) {
            const fullPath = path.join(targetDir, file);
            try {
                await fsPromises.unlink(fullPath);
                console.log(`🗑️  Deleted: ${file}`);
            } catch (err) {
                console.error(`❌ Failed to delete ${file}:`, err.message);
            }
        }
    }
     // Note: no filtering of fileIndex here — will be rebuild fully after sync to keep local files alive in index file
     // fileIndex = fileIndex.filter(entry => remoteFiles.has(entry.path));
}
// NEW helper to build index from all local .hd5 files
async function buildFullIndexFromLocal() {
    async function getAllHd5Files(dir) {
        const entries = await fsPromises.readdir(dir, { withFileTypes: true });
        let allFiles = [];
        for (const entry of entries) {
            const fullPath = path.join(dir, entry.name);
            if (entry.isDirectory()) {
                const nestedFiles = await getAllHd5Files(fullPath);
                allFiles = allFiles.concat(nestedFiles);
            } else if (entry.isFile() && fullPath.endsWith('-hd5')) {
                allFiles.push(fullPath);
            }
        }
        return allFiles;
    }

    const allLocalHd5Files = await getAllHd5Files(targetDir);
    const newIndex = allLocalHd5Files.map(fullPath => {
        const relPath = path.relative(targetDir, fullPath);
        const meta = extractInfoFromFilename(path.basename(fullPath));
        return meta ? { path: relPath, ...meta } : null;
    }).filter(Boolean);

    return newIndex;
}
// --- Main execution ---
async function startSync() {
    console.log(`🚀 Starting sync from ${hostURL}`);
    const remoteFiles = await syncDirectory(hostURL, targetDir, 12, 17);
    await cleanLocalFiles(remoteFiles);

    //await fsPromises.writeFile(indexFilePath, JSON.stringify(fileIndex, null, 2));
    //console.log(`✅ Sync complete. Index saved to ${indexFilePath}`);

    // Instead of only using fileIndex collected during download,
    // build an index of all files present locally:
    fileIndex = await buildFullIndexFromLocal();

    await fsPromises.writeFile(indexFilePath, JSON.stringify(fileIndex, null, 2));
    console.log(`✅ Sync complete. Index saved to ${indexFilePath}`);
}

startSync().catch(err => {
    console.error('💥 Fatal error:', err);
});
