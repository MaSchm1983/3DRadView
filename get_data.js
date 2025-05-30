const https = require('https');
const fs = require('fs');
const path = require('path');
const cheerio = require('cheerio');

const hostURL = 'https://opendata.dwd.de/weather/radar/sites/pz/'; // <-- Replace with your server URL
const targetDir = path.join(__dirname, '3D_DATA');
const META_FILE = path.join(targetDir, 'metadata.json');
const FILE_INDEX = path.join(targetDir, 'current_files.json');

// Create download directory if needed
if (!fs.existsSync(targetDir)) {
    fs.mkdirSync(targetDir, { recursive: true });
}

// Load existing metadata
let fileMeta = {};
let fileListIndex = [];

if (fs.existsSync(META_FILE)) {
    fileMeta = JSON.parse(fs.readFileSync(META_FILE, 'utf-8'));
}

// --- Extract Info from Filename ---
function extractInfoFromFilename(fileName) {
    try {
        if (fileName.length < 30) {
            console.warn(`Filename too short: ${fileName}`);
            return null;
        }

        const RadID = parseInt(fileName.substring(10, 15), 10);
        const timestamp = fileName.substring(16, 26);
        const site = fileName.substring(27, 30);

        if (isNaN(RadID)) {
            console.warn(`Invalid RadID in ${fileName}`);
            return null;
        }

        return { RadID, timestamp, site };
    } catch (err) {
        console.error(`Failed to parse filename: ${fileName}`, err.message);
        return null;
    }
}

// --- Fetch HTML Listing ---
function fetchDirectoryListing(url, callback) {
    https.get(url, (res) => {
        let html = '';
        res.on('data', chunk => html += chunk);
        res.on('end', () => {
            const $ = cheerio.load(html);
            const links = [];

            $('a').each((_, elem) => {
                const href = $(elem).attr('href');
                if (href && href !== '../') {
                    links.push(href);
                }
            });

            callback(links);
        });
    }).on('error', err => {
        console.error(`Failed to fetch ${url}:`, err.message);
        callback([]);
    });
}

// --- Check File for Update ---
function checkFileUpdate(fileUrl, localPath, callback) {
    const options = new URL(fileUrl);
    options.method = 'HEAD';

    https.request(options, res => {
        const remoteModified = res.headers['last-modified'];
        if (!remoteModified) {
            console.warn(`No Last-Modified header for ${fileUrl}`);
            return callback(false);
        }

        const relPath = path.relative(targetDir, localPath);
        const isUpdated = !fileMeta[relPath] || fileMeta[relPath] !== remoteModified;
        callback(isUpdated, remoteModified);
    }).on('error', err => {
        console.error(`HEAD error for ${fileUrl}:`, err.message);
        callback(false);
    }).end();
}

// --- Download File ---
function downloadFile(fileUrl, localPath, remoteModified, done) {
    const dir = path.dirname(localPath);
    if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
    }

    const fileStream = fs.createWriteStream(localPath);
    https.get(fileUrl, res => {
        res.pipe(fileStream);
        fileStream.on('finish', () => {
            fileStream.close();
            const relPath = path.relative(targetDir, localPath);
            fileMeta[relPath] = remoteModified;
            console.log(`Downloaded: ${relPath}`);
            processFilename(localPath, relPath);
            done();
        });
    }).on('error', err => {
        console.error(`Download error for ${fileUrl}:`, err.message);
        fs.unlink(localPath, () => {});
        done();
    });
}

// --- Parse Filename for Metadata ---
function processFilename(localPath, relPath) {
    const fileName = path.basename(localPath);
    const extracted = extractInfoFromFilename(fileName);
    if (extracted) {
        fileListIndex.push({
            path: relPath,
            ...extracted
        });
    }
}

// --- Recursive Sync ---
function syncDirectory(url, localDir, done) {
    fetchDirectoryListing(url, links => {
        let pending = links.length;
        if (pending === 0) return done();

        links.forEach(link => {
            const fullUrl = new URL(link, url).href;
            const isDir = link.endsWith('/');
            const localPath = path.join(localDir, decodeURIComponent(link));
            const relPath = path.relative(targetDir, localPath);

            if (isDir) {
                syncDirectory(fullUrl, localPath, () => {
                    pending--;
                    if (pending === 0) done();
                });
            } else {
                // Check and download if needed
                checkFileUpdate(fullUrl, localPath, (needsUpdate, remoteModified) => {
                    if (needsUpdate) {
                        downloadFile(fullUrl, localPath, remoteModified, () => {
                            pending--;
                            if (pending === 0) done();
                        });
                    } else {
                        console.log(`Up to date: ${relPath}`);
                        processFilename(localPath, relPath); // still extract info
                        pending--;
                        if (pending === 0) done();
                    }
                });
            }
        });
    });
}

// --- Start Sync Process ---
function startSync() {
    syncDirectory(hostURL, targetDir, () => {
        fs.writeFileSync(META_FILE, JSON.stringify(fileMeta, null, 2));
        fs.writeFileSync(FILE_INDEX, JSON.stringify(fileListIndex, null, 2));
        console.log('\n✅ Sync complete.');
        console.log(`📄 File index saved to: ${FILE_INDEX}`);
    });
}

startSync();
