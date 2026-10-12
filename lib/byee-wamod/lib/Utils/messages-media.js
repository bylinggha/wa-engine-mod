import { Boom } from '@hapi/boom';
import { exec } from 'child_process';
import * as Crypto from 'crypto';
import { once } from 'events';
import { createReadStream, createWriteStream, promises as fs, WriteStream } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { Readable, Transform } from 'stream';
import { URL } from 'url';
import { proto } from '../../WAProto/index.js';
import { DEFAULT_ORIGIN, MEDIA_HKDF_KEY_MAPPING, MEDIA_PATH_MAP } from '../Defaults/index.js';
import { getBinaryNodeChild, getBinaryNodeChildBuffer, jidNormalizedUser } from '../WABinary/index.js';
import { aesDecryptGCM, aesEncryptGCM, hkdf } from './crypto.js';
import { generateMessageIDV2 } from './generics.js';
const getTmpFilesDirectory = () => tmpdir();
const getImageProcessingLibrary = async () => {
    // ✅ OPTIMIZED: Prioritize Sharp (support webp, lebih cepat, lebih ringan)
    // ✅ Sharp support: jpeg, png, webp, gif, svg, dll
    //@ts-ignore
    const [sharp, jimp] = await Promise.all([
        import('sharp').catch(() => null), 
        import('jimp').catch(() => null)
    ]);
    if (sharp) {
        return { sharp };
    }
    if (jimp) {
        return { jimp };
    }
    throw new Boom('No image processing library available. Please install Sharp (recommended) or Jimp.');
};
export const hkdfInfoKey = (type) => {
    const hkdfInfo = MEDIA_HKDF_KEY_MAPPING[type];
    return `WhatsApp ${hkdfInfo} Keys`;
};
export const getRawMediaUploadData = async (media, mediaType, logger) => {
    const { stream } = await getStream(media);
    logger?.debug('got stream for raw upload');
    const hasher = Crypto.createHash('sha256');
    const filePath = join(tmpdir(), mediaType + generateMessageIDV2());
    const fileWriteStream = createWriteStream(filePath);
    let fileLength = 0;
    try {
        for await (const data of stream) {
            fileLength += data.length;
            hasher.update(data);
            if (!fileWriteStream.write(data)) {
                await once(fileWriteStream, 'drain');
            }
        }
        fileWriteStream.end();
        await once(fileWriteStream, 'finish');
        stream.destroy();
        const fileSha256 = hasher.digest();
        logger?.debug('hashed data for raw upload');
        return {
            filePath: filePath,
            fileSha256,
            fileLength
        };
    }
    catch (error) {
        fileWriteStream.destroy();
        stream.destroy();
        try {
            await fs.unlink(filePath);
        }
        catch {
            //
        }
        throw error;
    }
};
/** generates all the keys required to encrypt/decrypt & sign a media message */
export async function getMediaKeys(buffer, mediaType) {
    if (!buffer) {
        throw new Boom('Cannot derive from empty media key');
    }
    if (typeof buffer === 'string') {
        buffer = Buffer.from(buffer.replace('data:;base64,', ''), 'base64');
    }
    // expand using HKDF to 112 bytes, also pass in the relevant app info
    const expandedMediaKey = await hkdf(buffer, 112, { info: hkdfInfoKey(mediaType) });
    return {
        iv: expandedMediaKey.slice(0, 16),
        cipherKey: expandedMediaKey.slice(16, 48),
        macKey: expandedMediaKey.slice(48, 80)
    };
}
/** Extracts video thumb using FFMPEG */
const extractVideoThumb = async (path, destPath, time, size) => new Promise((resolve, reject) => {
    const cmd = `ffmpeg -ss ${time} -i ${path} -y -vf scale=${size.width}:-1 -vframes 1 -f image2 ${destPath}`;
    exec(cmd, err => {
        if (err) {
            reject(err);
        }
        else {
            resolve();
        }
    });
});
export const extractImageThumb = async (bufferOrFilePath, width = 32) => {
    // ✅ SMART: Handle file tidak ada atau corrupt dengan graceful fallback
    try {
        // TODO: Move entirely to sharp, removing jimp as it supports readable streams
        // This will have positive speed and performance impacts as well as minimizing RAM usage.
        if (bufferOrFilePath instanceof Readable) {
            bufferOrFilePath = await toBuffer(bufferOrFilePath);
        }
        
        // ✅ SMART: Check jika file path dan file tidak ada (dengan retry untuk race condition)
        if (typeof bufferOrFilePath === 'string' && !bufferOrFilePath.startsWith('http')) {
            let fileExists = false;
            let retryCount = 0;
            const maxRetries = 10; // ✅ INCREASED: 10 attempts untuk handle race condition yang lebih agresif
            const retryDelay = 200; // ✅ INCREASED: 200ms delay untuk give time file fully written
            
            // ✅ SMART: Retry check file existence (handle race condition saat file baru dibuat)
            while (!fileExists && retryCount < maxRetries) {
                try {
                    await fs.access(bufferOrFilePath);
                    // ✅ SMART: Double check - verify file size > 0 (file fully written)
                    const stats = await fs.stat(bufferOrFilePath);
                    if (stats.size > 0) {
                        // ✅ SMART: Triple check - verify file is readable (not locked)
                        try {
                            const testRead = await fs.readFile(bufferOrFilePath, { flag: 'r' });
                            if (testRead.length > 0) {
                                fileExists = true;
                                break;
                            }
                        } catch (readError) {
                            // File locked or not readable - wait and retry
                            if (retryCount < maxRetries - 1) {
                                await new Promise(resolve => setTimeout(resolve, retryDelay));
                                retryCount++;
                                continue;
                            }
                        }
                    } else {
                        // File exists but empty - wait a bit more
                        if (retryCount < maxRetries - 1) {
                            await new Promise(resolve => setTimeout(resolve, retryDelay));
                            retryCount++;
                        }
                    }
                } catch (accessError) {
                    // File doesn't exist yet - retry
                    if (retryCount < maxRetries - 1) {
                        await new Promise(resolve => setTimeout(resolve, retryDelay));
                        retryCount++;
                    } else {
                        // ✅ File tidak ada setelah retry - throw error yang jelas
                        throw new Boom(`File not found after ${maxRetries} attempts: ${bufferOrFilePath}`);
                    }
                }
            }
            
            if (!fileExists) {
                throw new Boom(`File not accessible or empty after ${maxRetries} attempts: ${bufferOrFilePath}`);
            }
        }
        
        const lib = await getImageProcessingLibrary();
        if ('sharp' in lib && typeof lib.sharp?.default === 'function') {
            try {
                const img = lib.sharp.default(bufferOrFilePath);
                const dimensions = await img.metadata();
                const buffer = await img.resize(width).jpeg({ quality: 50 }).toBuffer();
                return {
                    buffer,
                    original: {
                        width: dimensions.width,
                        height: dimensions.height
                    }
                };
            } catch (sharpError) {
                // ✅ SMART: Jika Sharp gagal (file corrupt/tidak ada), throw error yang jelas
                const errorMsg = sharpError.message || '';
                if (errorMsg.includes('unable to open') || 
                    errorMsg.includes('No such file') || 
                    errorMsg.includes('corrupt header') ||
                    errorMsg.includes('Input file is missing') ||
                    errorMsg.includes('file is missing')) {
                    throw new Boom(`File not accessible or missing: ${bufferOrFilePath}`);
                }
                throw sharpError;
            }
        }
        else if ('jimp' in lib) {
            // ✅ FIX: Handle jimp modern export (default function) or old export (Jimp object)
            const JimpClass = lib.jimp?.default || lib.jimp?.Jimp || lib.jimp;
            if (typeof JimpClass === 'function' && typeof JimpClass.read === 'function') {
                try {
                    const jimp = await JimpClass.read(bufferOrFilePath);
                    const dimensions = {
                        width: jimp.bitmap.width,
                        height: jimp.bitmap.height
                    };
                    // ✅ FIX: Jimp modern format - gunakan { w: width } saja (auto height)
                    const buffer = await jimp
                        .resize({ w: width })
                        .getBufferAsync('image/jpeg');
                    return {
                        buffer,
                        original: dimensions
                    };
                } catch (jimpError) {
                    // ✅ SMART: Jika Jimp juga gagal, throw error yang jelas
                    const errorMsg = jimpError.message || '';
                    if (errorMsg.includes('unable to open') || 
                        errorMsg.includes('No such file') || 
                        errorMsg.includes('corrupt') ||
                        errorMsg.includes('Input file is missing') ||
                        errorMsg.includes('file is missing')) {
                        throw new Boom(`File not accessible or missing: ${bufferOrFilePath}`);
                    }
                    throw jimpError;
                }
            } else if (typeof lib.jimp?.Jimp === 'object') {
                // Old jimp format (backward compatibility)
                try {
                    const jimp = await lib.jimp.Jimp.read(bufferOrFilePath);
                    const dimensions = {
                        width: jimp.width,
                        height: jimp.height
                    };
                    const buffer = await jimp
                        .resize({ w: width, mode: lib.jimp.ResizeStrategy.BILINEAR })
                        .getBuffer('image/jpeg', { quality: 50 });
                    return {
                        buffer,
                        original: dimensions
                    };
                } catch (jimpError) {
                    const errorMsg = jimpError.message || '';
                    if (errorMsg.includes('unable to open') || 
                        errorMsg.includes('No such file') || 
                        errorMsg.includes('corrupt') ||
                        errorMsg.includes('Input file is missing') ||
                        errorMsg.includes('file is missing')) {
                        throw new Boom(`File not accessible or missing: ${bufferOrFilePath}`);
                    }
                    throw jimpError;
                }
            }
        }
        else {
            throw new Boom('No image processing library available');
        }
    } catch (error) {
        // ✅ SMART: Re-throw dengan error message yang jelas
        if (error instanceof Boom) {
            throw error;
        }
        throw new Boom(`Failed to extract image thumbnail: ${error.message}`);
    }
};
export const encodeBase64EncodedStringForUpload = (b64) => encodeURIComponent(b64.replace(/\+/g, '-').replace(/\//g, '_').replace(/\=+$/, ''));
export const generateProfilePicture = async (mediaUpload, dimensions) => {
    let buffer;
    const { width: w = 640, height: h = 640 } = dimensions || {};
    if (Buffer.isBuffer(mediaUpload)) {
        buffer = mediaUpload;
    }
    else {
        // Use getStream to handle all WAMediaUpload types (Buffer, Stream, URL)
        const { stream } = await getStream(mediaUpload);
        // Convert the resulting stream to a buffer
        buffer = await toBuffer(stream);
    }
    const lib = await getImageProcessingLibrary();
    let img;
    if ('sharp' in lib && typeof lib.sharp?.default === 'function') {
        img = lib.sharp
            .default(buffer)
            .resize(w, h)
            .jpeg({
            quality: 50
        })
            .toBuffer();
    }
    else if ('jimp' in lib && typeof lib.jimp?.Jimp === 'object') {
        const jimp = await lib.jimp.Jimp.read(buffer);
        const min = Math.min(jimp.width, jimp.height);
        const cropped = jimp.crop({ x: 0, y: 0, w: min, h: min });
        img = cropped.resize({ w, h, mode: lib.jimp.ResizeStrategy.BILINEAR }).getBuffer('image/jpeg', { quality: 50 });
    }
    else {
        throw new Boom('No image processing library available');
    }
    return {
        img: await img
    };
};
/** gets the SHA256 of the given media message */
export const mediaMessageSHA256B64 = (message) => {
    const media = Object.values(message)[0];
    return media?.fileSha256 && Buffer.from(media.fileSha256).toString('base64');
};
export async function getAudioDuration(buffer) {
    const musicMetadata = await import('music-metadata');
    let metadata;
    const options = {
        duration: true
    };
    if (Buffer.isBuffer(buffer)) {
        metadata = await musicMetadata.parseBuffer(buffer, undefined, options);
    }
    else if (typeof buffer === 'string') {
        metadata = await musicMetadata.parseFile(buffer, options);
    }
    else {
        metadata = await musicMetadata.parseStream(buffer, undefined, options);
    }
    return metadata.format.duration;
}
/**
  referenced from and modifying https://github.com/wppconnect-team/wa-js/blob/main/src/chat/functions/prepareAudioWaveform.ts
 */
export async function getAudioWaveform(buffer, logger) {
    try {
        // @ts-ignore
        const { default: decoder } = await import('audio-decode');
        let audioData;
        if (Buffer.isBuffer(buffer)) {
            audioData = buffer;
        }
        else if (typeof buffer === 'string') {
            const rStream = createReadStream(buffer);
            audioData = await toBuffer(rStream);
        }
        else {
            audioData = await toBuffer(buffer);
        }
        const audioBuffer = await decoder(audioData);
        const rawData = audioBuffer.getChannelData(0); // We only need to work with one channel of data
        const samples = 64; // Number of samples we want to have in our final data set
        const blockSize = Math.floor(rawData.length / samples); // the number of samples in each subdivision
        const filteredData = [];
        for (let i = 0; i < samples; i++) {
            const blockStart = blockSize * i; // the location of the first sample in the block
            let sum = 0;
            for (let j = 0; j < blockSize; j++) {
                sum = sum + Math.abs(rawData[blockStart + j]); // find the sum of all the samples in the block
            }
            filteredData.push(sum / blockSize); // divide the sum by the block size to get the average
        }
        // This guarantees that the largest data point will be set to 1, and the rest of the data will scale proportionally.
        const multiplier = Math.pow(Math.max(...filteredData), -1);
        const normalizedData = filteredData.map(n => n * multiplier);
        // Generate waveform like WhatsApp
        const waveform = new Uint8Array(normalizedData.map(n => Math.floor(100 * n)));
        return waveform;
    }
    catch (e) {
        logger?.debug('Failed to generate waveform: ' + e);
    }
}
export const toReadable = (buffer) => {
    const readable = new Readable({ read: () => { } });
    readable.push(buffer);
    readable.push(null);
    return readable;
};
export const toBuffer = async (stream) => {
    const chunks = [];
    for await (const chunk of stream) {
        chunks.push(chunk);
    }
    stream.destroy();
    return Buffer.concat(chunks);
};
export const getStream = async (item, opts) => {
    if (Buffer.isBuffer(item)) {
        return { stream: toReadable(item), type: 'buffer' };
    }
    if ('stream' in item) {
        return { stream: item.stream, type: 'readable' };
    }
    const urlStr = item.url.toString();
    if (urlStr.startsWith('data:')) {
        const buffer = Buffer.from(urlStr.split(',')[1], 'base64');
        return { stream: toReadable(buffer), type: 'buffer' };
    }
    if (urlStr.startsWith('http://') || urlStr.startsWith('https://')) {
        return { stream: await getHttpStream(item.url, opts), type: 'remote' };
    }
    return { stream: createReadStream(item.url), type: 'file' };
};
/** generates a thumbnail for a given media, if required */
export async function generateThumbnail(file, mediaType, options) {
    let thumbnail;
    let originalImageDimensions;
    if (mediaType === 'image') {
        // ✅ FIX THUMBNAIL SIZE: Generate thumbnail dengan size 192px (dari 32px default)
        // ✅ Size 192px sama seperti link preview - lebih visible untuk preview
        // ✅ DEBUG: Gunakan console.error (tidak di-disable) untuk debugging - DETAIL LOGGING
        const timestamp = new Date().toISOString();
        console.error(`[THUMBNAIL-DEBUG] ${timestamp} | ACTION: Generating thumbnail 192px from file`);
        console.error(`[THUMBNAIL-DEBUG] ${timestamp} | FilePath: ${file}`);
        console.error(`[THUMBNAIL-DEBUG] ${timestamp} | ThumbnailSize: 192px`);
        
        try {
            const { buffer, original } = await extractImageThumb(file, 192);
            thumbnail = buffer.toString('base64');
            const thumbSizeKB = Math.round(thumbnail.length / 1024);
            console.error(`[THUMBNAIL-DEBUG] ${timestamp} | STATUS: Thumbnail generated successfully`);
            console.error(`[THUMBNAIL-DEBUG] ${timestamp} | ThumbnailSizeBase64: ${thumbSizeKB}KB (${thumbnail.length} chars)`);
            console.error(`[THUMBNAIL-DEBUG] ${timestamp} | OriginalSize: ${original?.width || 'unknown'}x${original?.height || 'unknown'}`);
            if (original.width && original.height) {
                originalImageDimensions = {
                    width: original.width,
                    height: original.height
                };
                console.error(`[THUMBNAIL-DEBUG] ${timestamp} | Dimensions: ${original.width}x${original.height}`);
            }
        } catch (error) {
            // ✅ SMART: Jika thumbnail gagal (file tidak ada/corrupt), skip thumbnail tapi tetap lanjut
            const errorMsg = error.message || error.toString() || '';
            console.error(`[THUMBNAIL-DEBUG] ${timestamp} | ERROR: Failed to generate thumbnail`);
            console.error(`[THUMBNAIL-DEBUG] ${timestamp} | ErrorMessage: ${errorMsg}`);
            console.error(`[THUMBNAIL-DEBUG] ${timestamp} | ErrorStack: ${error.stack || 'N/A'}`);
            console.error(`[THUMBNAIL-DEBUG] ${timestamp} | STATUS: Continuing without thumbnail`);
            // ✅ SMART: Set thumbnail ke undefined (tidak throw error) - pesan tetap bisa dikirim tanpa thumbnail
            thumbnail = undefined;
            options.logger?.debug(`Could not generate thumbnail: ${errorMsg}`);
        }
    }
    else if (mediaType === 'video') {
        const imgFilename = join(getTmpFilesDirectory(), generateMessageIDV2() + '.jpg');
        try {
            await extractVideoThumb(file, imgFilename, '00:00:00', { width: 32, height: 32 });
            const buff = await fs.readFile(imgFilename);
            thumbnail = buff.toString('base64');
            await fs.unlink(imgFilename);
        }
        catch (err) {
            options.logger?.debug('could not generate video thumb: ' + err);
        }
    }
    return {
        thumbnail,
        originalImageDimensions
    };
}
export const getHttpStream = async (url, options = {}) => {
    const response = await fetch(url.toString(), {
        dispatcher: options.dispatcher,
        method: 'GET',
        headers: options.headers
    });
    if (!response.ok) {
        throw new Boom(`Failed to fetch stream from ${url}`, { statusCode: response.status, data: { url } });
    }
    // @ts-ignore Node18+ Readable.fromWeb exists
    return Readable.fromWeb(response.body);
};
export const encryptedStream = async (media, mediaType, { logger, saveOriginalFileIfRequired, opts } = {}) => {
    const { stream, type } = await getStream(media, opts);
    logger?.debug('fetched media stream');
    const mediaKey = Crypto.randomBytes(32);
    const { cipherKey, iv, macKey } = await getMediaKeys(mediaKey, mediaType);
    const encFilePath = join(getTmpFilesDirectory(), mediaType + generateMessageIDV2() + '-enc');
    const encFileWriteStream = createWriteStream(encFilePath);
    let originalFileStream;
    let originalFilePath;
    if (saveOriginalFileIfRequired) {
        originalFilePath = join(getTmpFilesDirectory(), mediaType + generateMessageIDV2() + '-original');
        originalFileStream = createWriteStream(originalFilePath);
    }
    let fileLength = 0;
    const aes = Crypto.createCipheriv('aes-256-cbc', cipherKey, iv);
    const hmac = Crypto.createHmac('sha256', macKey).update(iv);
    const sha256Plain = Crypto.createHash('sha256');
    const sha256Enc = Crypto.createHash('sha256');
    const onChunk = (buff) => {
        sha256Enc.update(buff);
        hmac.update(buff);
        encFileWriteStream.write(buff);
    };
    try {
        for await (const data of stream) {
            fileLength += data.length;
            if (type === 'remote' &&
                opts?.maxContentLength &&
                fileLength + data.length > opts.maxContentLength) {
                throw new Boom(`content length exceeded when encrypting "${type}"`, {
                    data: { media, type }
                });
            }
            if (originalFileStream) {
                if (!originalFileStream.write(data)) {
                    await once(originalFileStream, 'drain');
                }
            }
            sha256Plain.update(data);
            onChunk(aes.update(data));
        }
        onChunk(aes.final());
        const mac = hmac.digest().slice(0, 10);
        sha256Enc.update(mac);
        const fileSha256 = sha256Plain.digest();
        const fileEncSha256 = sha256Enc.digest();
        encFileWriteStream.write(mac);
        encFileWriteStream.end();
        originalFileStream?.end?.();
        stream.destroy();
        logger?.debug('encrypted data successfully');
        return {
            mediaKey,
            originalFilePath,
            encFilePath,
            mac,
            fileEncSha256,
            fileSha256,
            fileLength
        };
    }
    catch (error) {
        // destroy all streams with error
        encFileWriteStream.destroy();
        originalFileStream?.destroy?.();
        aes.destroy();
        hmac.destroy();
        sha256Plain.destroy();
        sha256Enc.destroy();
        stream.destroy();
        try {
            await fs.unlink(encFilePath);
            if (originalFilePath) {
                await fs.unlink(originalFilePath);
            }
        }
        catch (err) {
            logger?.error({ err }, 'failed deleting tmp files');
        }
        throw error;
    }
};
const DEF_HOST = 'mmg.whatsapp.net';
const AES_CHUNK_SIZE = 16;
const toSmallestChunkSize = (num) => {
    return Math.floor(num / AES_CHUNK_SIZE) * AES_CHUNK_SIZE;
};
export const getUrlFromDirectPath = (directPath) => `https://${DEF_HOST}${directPath}`;
export const downloadContentFromMessage = async ({ mediaKey, directPath, url }, type, opts = {}) => {
    const isValidMediaUrl = url?.startsWith('https://mmg.whatsapp.net/');
    const downloadUrl = isValidMediaUrl ? url : getUrlFromDirectPath(directPath);
    if (!downloadUrl) {
        throw new Boom('No valid media URL or directPath present in message', { statusCode: 400 });
    }
    const keys = await getMediaKeys(mediaKey, type);
    return downloadEncryptedContent(downloadUrl, keys, opts);
};
/**
 * Decrypts and downloads an AES256-CBC encrypted file given the keys.
 * Assumes the SHA256 of the plaintext is appended to the end of the ciphertext
 * */
export const downloadEncryptedContent = async (downloadUrl, { cipherKey, iv }, { startByte, endByte, options } = {}) => {
    let bytesFetched = 0;
    let startChunk = 0;
    let firstBlockIsIV = false;
    // if a start byte is specified -- then we need to fetch the previous chunk as that will form the IV
    if (startByte) {
        const chunk = toSmallestChunkSize(startByte || 0);
        if (chunk) {
            startChunk = chunk - AES_CHUNK_SIZE;
            bytesFetched = chunk;
            firstBlockIsIV = true;
        }
    }
    const endChunk = endByte ? toSmallestChunkSize(endByte || 0) + AES_CHUNK_SIZE : undefined;
    const headersInit = options?.headers ? options.headers : undefined;
    const headers = {
        ...(headersInit
            ? Array.isArray(headersInit)
                ? Object.fromEntries(headersInit)
                : headersInit
            : {}),
        Origin: DEFAULT_ORIGIN
    };
    if (startChunk || endChunk) {
        headers.Range = `bytes=${startChunk}-`;
        if (endChunk) {
            headers.Range += endChunk;
        }
    }
    // download the message
    const fetched = await getHttpStream(downloadUrl, {
        ...(options || {}),
        headers
    });
    let remainingBytes = Buffer.from([]);
    let aes;
    const pushBytes = (bytes, push) => {
        if (startByte || endByte) {
            const start = bytesFetched >= startByte ? undefined : Math.max(startByte - bytesFetched, 0);
            const end = bytesFetched + bytes.length < endByte ? undefined : Math.max(endByte - bytesFetched, 0);
            push(bytes.slice(start, end));
            bytesFetched += bytes.length;
        }
        else {
            push(bytes);
        }
    };
    const output = new Transform({
        transform(chunk, _, callback) {
            let data = Buffer.concat([remainingBytes, chunk]);
            const decryptLength = toSmallestChunkSize(data.length);
            remainingBytes = data.slice(decryptLength);
            data = data.slice(0, decryptLength);
            if (!aes) {
                let ivValue = iv;
                if (firstBlockIsIV) {
                    ivValue = data.slice(0, AES_CHUNK_SIZE);
                    data = data.slice(AES_CHUNK_SIZE);
                }
                aes = Crypto.createDecipheriv('aes-256-cbc', cipherKey, ivValue);
                // if an end byte that is not EOF is specified
                // stop auto padding (PKCS7) -- otherwise throws an error for decryption
                if (endByte) {
                    aes.setAutoPadding(false);
                }
            }
            try {
                pushBytes(aes.update(data), b => this.push(b));
                callback();
            }
            catch (error) {
                callback(error);
            }
        },
        final(callback) {
            try {
                pushBytes(aes.final(), b => this.push(b));
                callback();
            }
            catch (error) {
                callback(error);
            }
        }
    });
    return fetched.pipe(output, { end: true });
};
export function extensionForMediaMessage(message) {
    const getExtension = (mimetype) => mimetype.split(';')[0]?.split('/')[1];
    const type = Object.keys(message)[0];
    let extension;
    if (type === 'locationMessage' || type === 'liveLocationMessage' || type === 'productMessage') {
        extension = '.jpeg';
    }
    else {
        const messageContent = message[type];
        extension = getExtension(messageContent.mimetype);
    }
    return extension;
}
// ✅ SMART: Cache untuk upload URL (key: fileEncSha256B64 + mediaType, value: upload result)
const uploadCache = new Map();
const CACHE_TTL = 60 * 60 * 1000; // 1 hour
const MAX_CACHE_SIZE = 1000; // ✅ SMART: Limit cache size untuk prevent memory leak

// ✅ SMART: Cleanup expired cache entries setiap 10 menit + enforce max size
setInterval(() => {
    const now = Date.now();
    const entries = Array.from(uploadCache.entries());
    
    // ✅ SMART: Remove expired entries
    for (const [key, value] of entries) {
        if (now - value.timestamp > CACHE_TTL) {
            uploadCache.delete(key);
        }
    }
    
    // ✅ SMART: If still over limit, remove oldest entries (LRU-like)
    if (uploadCache.size > MAX_CACHE_SIZE) {
        const sortedEntries = Array.from(uploadCache.entries())
            .sort((a, b) => a[1].timestamp - b[1].timestamp);
        
        const toDelete = sortedEntries.slice(0, uploadCache.size - MAX_CACHE_SIZE);
        for (const [key] of toDelete) {
            uploadCache.delete(key);
        }
    }
}, 10 * 60 * 1000);

export const getWAUploadToServer = ({ customUploadHosts, fetchAgent, logger, options }, refreshMediaConn) => {
    return async (filePath, { mediaType, fileEncSha256B64, timeoutMs }) => {
        // ✅ SMART: Check cache first (key: fileEncSha256B64 + mediaType)
        const cacheKey = `${fileEncSha256B64}_${mediaType}`;
        const cached = uploadCache.get(cacheKey);
        
        if (cached && (Date.now() - cached.timestamp) < CACHE_TTL) {
            logger.debug(`✅ Using cached upload result for ${mediaType} (${fileEncSha256B64.substring(0, 16)}...)`);
            return cached.urls;
        }
        
        // ✅ SMART: Check file exists before upload
        try {
            await fs.access(filePath);
        } catch (accessError) {
            throw new Boom(`Media file not found: ${filePath}`, { statusCode: 404 });
        }

        // ✅ SMART & FAST: Langsung fetch, jika error langsung throw (tidak retry)
        let uploadInfo;
        try {
            uploadInfo = await refreshMediaConn(false);
        } catch (error) {
            // ✅ FAST: Langsung throw error (tidak retry)
            const errorMsg = error.message || error.toString() || '';
            if (errorMsg.includes('1006') || errorMsg.includes('Connection Closed') || errorMsg.includes('connection closed')) {
                throw new Error(`Cannot send image: Connection issue (${errorMsg}). Please retry when connection is stable.`);
            }
            throw error;
        }
        
        // ✅ FAST: Validate hosts, jika tidak ada langsung throw (tidak retry)
        if (!uploadInfo || !uploadInfo.hosts || uploadInfo.hosts.length === 0) {
            logger.error({ uploadInfo, customUploadHosts }, 'No upload hosts available');
            throw new Boom('No upload hosts available. Please check your connection and try again.', { statusCode: 500 });
        }
        
        let urls;
        const hosts = [...customUploadHosts, ...uploadInfo.hosts];

        fileEncSha256B64 = encodeBase64EncodedStringForUpload(fileEncSha256B64);
        let lastError = null;
        let lastResult = null;
        
        for (let i = 0; i < hosts.length; i++) {
            const { hostname } = hosts[i];
            const isLast = i === hosts.length - 1;
            
            logger.debug(`uploading to "${hostname}" (${i + 1}/${hosts.length})`);
            const auth = encodeURIComponent(uploadInfo.auth); // the auth token
            const url = `https://${hostname}${MEDIA_PATH_MAP[mediaType]}/${fileEncSha256B64}?auth=${auth}&token=${fileEncSha256B64}`;
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            let result;
            try {
                const stream = createReadStream(filePath);
                const response = await fetch(url, {
                    dispatcher: fetchAgent,
                    method: 'POST',
                    body: stream,
                    headers: {
                        ...(() => {
                            const hdrs = options?.headers;
                            if (!hdrs)
                                return {};
                            return Array.isArray(hdrs) ? Object.fromEntries(hdrs) : hdrs;
                        })(),
                        'Content-Type': 'application/octet-stream',
                        Origin: DEFAULT_ORIGIN
                    },
                    duplex: 'half',
                    // Note: custom agents/proxy require undici Agent; omitted here.
                    signal: timeoutMs ? AbortSignal.timeout(timeoutMs) : undefined
                });
                
                // ✅ OPTIMIZED: Check response status dan content-type dulu (lebih cepat)
                // ✅ Hanya read response text jika status tidak OK atau content-type bukan JSON
                const contentType = response.headers.get('content-type') || '';
                const isJsonResponse = contentType.includes('application/json');
                
                // ✅ OPTIMIZED: Jika status OK dan content-type JSON, langsung parse (tidak perlu read text dulu)
                if (response.ok && isJsonResponse) {
                    // ✅ Langsung parse JSON tanpa read text dulu (lebih cepat)
                    try {
                        result = await response.json();
                    } catch (jsonError) {
                        // ✅ Jika JSON parse gagal, baru read text untuk debugging
                        const responseText = await response.text().catch(() => 'Unable to read response');
                        logger.warn({ 
                            jsonError: jsonError.message, 
                            responseText: responseText.substring(0, 200),
                            contentType 
                        }, 'Failed to parse JSON response');
                        throw new Error(`Invalid JSON response: ${jsonError.message}. Raw response: ${responseText.substring(0, 100)}`);
                    }
                } else {
                    // ✅ Hanya read text jika status tidak OK atau bukan JSON (untuk detect "error code: 502")
                    const responseText = await response.text().catch(() => 'Unable to read response');
                    
                    // ✅ CRITICAL FIX: Detect gateway errors di response text (bisa status 200 tapi body "error code: 502")
                    if (responseText && typeof responseText === 'string') {
                        const lowerText = responseText.toLowerCase().trim();
                        // ✅ Check specific patterns untuk prevent false positive
                        if (lowerText.includes('error code: 502') || lowerText.includes('bad gateway')) {
                            logger.warn({ responseText: responseText.substring(0, 200), status: response.status }, 'Gateway error (502) detected in response text');
                            throw new Error(`Gateway error (502): ${responseText.substring(0, 100)}`);
                        }
                        if (lowerText.includes('error code: 504') || lowerText.includes('gateway timeout')) {
                            logger.warn({ responseText: responseText.substring(0, 200), status: response.status }, 'Gateway timeout (504) detected in response text');
                            throw new Error(`Gateway timeout (504): ${responseText.substring(0, 100)}`);
                        }
                    }
                    
                    // ✅ Handle non-OK responses
                    if (!response.ok) {
                        throw new Error(`HTTP ${response.status}: ${responseText.substring(0, 200)}`);
                    }
                    
                    // ✅ Jika bukan JSON, throw error
                    logger.warn({ contentType, responseText: responseText.substring(0, 200), status: response.status }, 'Non-JSON response received');
                    throw new Error(`Expected JSON but got ${contentType}. Response: ${responseText.substring(0, 100)}`);
                }
                
                // ✅ result sudah di-set di atas (langsung dari response.json() atau error sudah di-throw)
                
                if (result?.url || result?.directPath) {
                    urls = {
                        mediaUrl: result.url,
                        directPath: result.direct_path,
                        meta_hmac: result.meta_hmac,
                        fbid: result.fbid,
                        ts: result.ts
                    };
                    logger.debug(`✅ Upload successful to "${hostname}"`);
                    
                    // ✅ SMART: Cache hasil upload untuk reuse
                    uploadCache.set(cacheKey, {
                        urls,
                        timestamp: Date.now()
                    });
                    logger.debug(`💾 Cached upload result for ${mediaType} (${fileEncSha256B64.substring(0, 16)}...)`);
                    break;
                }
                else {
                    // ✅ SMART: Refresh media connection before retrying
                    if (!isLast) {
                        uploadInfo = await refreshMediaConn(true);
                    }
                    throw new Error(`upload failed, reason: ${JSON.stringify(result)}`);
                }
            }
            catch (error) {
                lastError = error;
                lastResult = result;
                const errorMsg = error.message || error.toString();
                
                // ✅ CRITICAL FIX: Comprehensive gateway error detection (termasuk "error code: 502" di response text)
                const isGatewayError = errorMsg.includes('Gateway error (502)') || 
                                      errorMsg.includes('Gateway timeout (504)') ||
                                      errorMsg.includes('error code: 502') ||
                                      errorMsg.includes('error code: 504') ||
                                      errorMsg.includes('Gateway timeout') ||
                                      errorMsg.includes('Bad gateway') ||
                                      errorMsg.includes('504') || 
                                      errorMsg.includes('502');
                
                logger.warn({ 
                    trace: error?.stack, 
                    uploadResult: result,
                    hostname,
                    attempt: i + 1,
                    total: hosts.length,
                    isGatewayError
                }, `Error in uploading to ${hostname}${isLast ? ' (last host)' : ', retrying...'}`);
                
                // ✅ SMART: If last host, don't continue
                if (isLast) {
                    break;
                }
                
                // ✅ SMART: Untuk gateway error, refresh media conn sebelum retry host berikutnya
                if (isGatewayError && !isLast) {
                    try {
                        uploadInfo = await refreshMediaConn(true);
                        logger.debug('Refreshed media connection after gateway error');
                    } catch (refreshError) {
                        logger.warn({ refreshError: refreshError.message }, 'Failed to refresh media connection');
                    }
                }
            }
        }
        
        if (!urls) {
            // ✅ SMART: Provide detailed error message
            const errorDetails = {
                filePath,
                mediaType,
                hostsAttempted: hosts.length,
                lastError: lastError?.message || 'Unknown error',
                lastResult: lastResult
            };
            logger.error(errorDetails, 'Media upload failed on all hosts');
            throw new Boom(`Media upload failed on all hosts (${hosts.length} hosts attempted). Last error: ${lastError?.message || 'Unknown'}`, { 
                statusCode: 500,
                data: errorDetails
            });
        }
        
        // ✅ SMART: Ensure cache is set (fallback jika belum di-set di break statement)
        if (!uploadCache.has(cacheKey)) {
            uploadCache.set(cacheKey, {
                urls,
                timestamp: Date.now()
            });
            logger.debug(`💾 Cached upload result (fallback) for ${mediaType}`);
        }
        
        return urls;
    };
};
const getMediaRetryKey = (mediaKey) => {
    return hkdf(mediaKey, 32, { info: 'WhatsApp Media Retry Notification' });
};
/**
 * Generate a binary node that will request the phone to re-upload the media & return the newly uploaded URL
 */
export const encryptMediaRetryRequest = async (key, mediaKey, meId) => {
    const recp = { stanzaId: key.id };
    const recpBuffer = proto.ServerErrorReceipt.encode(recp).finish();
    const iv = Crypto.randomBytes(12);
    const retryKey = await getMediaRetryKey(mediaKey);
    const ciphertext = aesEncryptGCM(recpBuffer, retryKey, iv, Buffer.from(key.id));
    const req = {
        tag: 'receipt',
        attrs: {
            id: key.id,
            to: jidNormalizedUser(meId),
            type: 'server-error'
        },
        content: [
            // this encrypt node is actually pretty useless
            // the media is returned even without this node
            // keeping it here to maintain parity with WA Web
            {
                tag: 'encrypt',
                attrs: {},
                content: [
                    { tag: 'enc_p', attrs: {}, content: ciphertext },
                    { tag: 'enc_iv', attrs: {}, content: iv }
                ]
            },
            {
                tag: 'rmr',
                attrs: {
                    jid: key.remoteJid,
                    from_me: (!!key.fromMe).toString(),
                    // @ts-ignore
                    participant: key.participant || undefined
                }
            }
        ]
    };
    return req;
};
export const decodeMediaRetryNode = (node) => {
    const rmrNode = getBinaryNodeChild(node, 'rmr');
    const event = {
        key: {
            id: node.attrs.id,
            remoteJid: rmrNode.attrs.jid,
            fromMe: rmrNode.attrs.from_me === 'true',
            participant: rmrNode.attrs.participant
        }
    };
    const errorNode = getBinaryNodeChild(node, 'error');
    if (errorNode) {
        const errorCode = +errorNode.attrs.code;
        event.error = new Boom(`Failed to re-upload media (${errorCode})`, {
            data: errorNode.attrs,
            statusCode: getStatusCodeForMediaRetry(errorCode)
        });
    }
    else {
        const encryptedInfoNode = getBinaryNodeChild(node, 'encrypt');
        const ciphertext = getBinaryNodeChildBuffer(encryptedInfoNode, 'enc_p');
        const iv = getBinaryNodeChildBuffer(encryptedInfoNode, 'enc_iv');
        if (ciphertext && iv) {
            event.media = { ciphertext, iv };
        }
        else {
            event.error = new Boom('Failed to re-upload media (missing ciphertext)', { statusCode: 404 });
        }
    }
    return event;
};
export const decryptMediaRetryData = async ({ ciphertext, iv }, mediaKey, msgId) => {
    const retryKey = await getMediaRetryKey(mediaKey);
    const plaintext = aesDecryptGCM(ciphertext, retryKey, iv, Buffer.from(msgId));
    return proto.MediaRetryNotification.decode(plaintext);
};
export const getStatusCodeForMediaRetry = (code) => MEDIA_RETRY_STATUS_MAP[code];
const MEDIA_RETRY_STATUS_MAP = {
    [proto.MediaRetryNotification.ResultType.SUCCESS]: 200,
    [proto.MediaRetryNotification.ResultType.DECRYPTION_ERROR]: 412,
    [proto.MediaRetryNotification.ResultType.NOT_FOUND]: 404,
    [proto.MediaRetryNotification.ResultType.GENERAL_ERROR]: 418
};
//# sourceMappingURL=messages-media.js.map