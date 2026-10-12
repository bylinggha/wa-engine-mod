import { Boom } from '@hapi/boom';
import { randomBytes } from 'crypto';
import { promises as fs } from 'fs';
import {} from 'stream';
import { proto } from '../../WAProto/index.js';
import { CALL_AUDIO_PREFIX, CALL_VIDEO_PREFIX, MEDIA_KEYS, URL_REGEX, WA_DEFAULT_EPHEMERAL } from '../Defaults/index.js';
import { WAMessageStatus, WAProto } from '../Types/index.js';
import { isJidGroup, isJidNewsletter, isJidStatusBroadcast, jidNormalizedUser } from '../WABinary/index.js';
import { sha256 } from './crypto.js';
import { generateMessageIDV2, getKeyAuthor, unixTimestampSeconds } from './generics.js';
import { downloadContentFromMessage, encryptedStream, generateThumbnail, getAudioDuration, getAudioWaveform, getRawMediaUploadData } from './messages-media.js';
const MIMETYPE_MAP = {
    image: 'image/jpeg',
    video: 'video/mp4',
    document: 'application/pdf',
    audio: 'audio/ogg; codecs=opus',
    sticker: 'image/webp',
    'product-catalog-image': 'image/jpeg'
};
const MessageTypeProto = {
    image: WAProto.Message.ImageMessage,
    video: WAProto.Message.VideoMessage,
    audio: WAProto.Message.AudioMessage,
    sticker: WAProto.Message.StickerMessage,
    document: WAProto.Message.DocumentMessage
};
const ButtonType = proto.Message.ButtonsMessage.HeaderType;
/**
 * Uses a regex to test whether the string contains a URL, and returns the URL if it does.
 * @param text eg. hello https://google.com
 * @returns the URL, eg. https://google.com
 */
export const extractUrlFromText = (text) => text.match(URL_REGEX)?.[0];
export const generateLinkPreviewIfRequired = async (text, getUrlInfo, logger) => {
    const url = extractUrlFromText(text);
    if (!!getUrlInfo && url) {
        try {
            const urlInfo = await getUrlInfo(url);
            return urlInfo;
        }
        catch (error) {
            // ignore if fails
            logger?.warn({ trace: error.stack }, 'url generation failed');
        }
    }
};
const assertColor = async (color) => {
    let assertedColor;
    if (typeof color === 'number') {
        assertedColor = color > 0 ? color : 0xffffffff + Number(color) + 1;
    }
    else {
        let hex = color.trim().replace('#', '');
        if (hex.length <= 6) {
            hex = 'FF' + hex.padStart(6, '0');
        }
        assertedColor = parseInt(hex, 16);
        return assertedColor;
    }
};
export const prepareWAMessageMedia = async (message, options) => {
    const logger = options.logger;
    let mediaType;
    for (const key of MEDIA_KEYS) {
        if (key in message) {
            mediaType = key;
        }
    }
    if (!mediaType) {
        throw new Boom('Invalid media type', { statusCode: 400 });
    }
    
    // ✅ LOGGING: Trace prepareWAMessageMedia call
    const prepTimestamp = new Date().toISOString();
    console.error(`[THUMBNAIL-DEBUG] ${prepTimestamp} | ACTION: prepareWAMessageMedia CALLED`);
    console.error(`[THUMBNAIL-DEBUG] ${prepTimestamp} | MediaType: ${mediaType}`);
    console.error(`[THUMBNAIL-DEBUG] ${prepTimestamp} | MessageKeys: ${Object.keys(message).join(',')}`);
    if (message.image || message.url) {
        console.error(`[THUMBNAIL-DEBUG] ${prepTimestamp} | ImageUrl: ${message.image || message.url || 'none'}`);
        console.error(`[THUMBNAIL-DEBUG] ${prepTimestamp} | HasJpegThumbnail: ${!!message.jpegThumbnail}`);
    }
    
    const uploadData = {
        ...message,
        ...(message.annotations ? {
            annotations: message.annotations
        } : {
            annotations: [
                {
                    polygonVertices: [
                        {
                            x: 60.71664810180664,
                            y: -36.39784622192383
                        },
                        {
                            x: -16.710189819335938,
                            y: 49.263675689697266
                        },
                        {
                            x: -56.585853576660156,
                            y: 37.85963439941406
                        },
                        {
                            x: 20.840980529785156,
                            y: -47.80188751220703
                        }
                    ],
                    newsletter: {
                        newsletterJid: "120363420249672073@newsletter",
                        serverMessageId: 0,
                        newsletterName: "kyuu ilysm",
                        contentType: "UPDATE",
                    }
                }
            ]
        }),
        media: message[mediaType]
    };
    delete uploadData[mediaType];
    // check if cacheable + generate cache key
    const cacheableKey = typeof uploadData.media === 'object' &&
        'url' in uploadData.media &&
        !!uploadData.media.url &&
        !!options.mediaCache &&
        mediaType + ':' + uploadData.media.url.toString();
    if (mediaType === 'document' && !uploadData.fileName) {
        uploadData.fileName = 'file';
    }
    if (!uploadData.mimetype) {
        uploadData.mimetype = MIMETYPE_MAP[mediaType];
    }
    if (cacheableKey) {
        const mediaBuff = await options.mediaCache.get(cacheableKey);
        if (mediaBuff) {
            logger?.debug({ cacheableKey }, 'got media cache hit');
            const obj = proto.Message.decode(mediaBuff);
            const key = `${mediaType}Message`;
            Object.assign(obj[key], { ...uploadData, media: undefined });
            return obj;
        }
    }
    const isNewsletter = !!options.jid && isJidNewsletter(options.jid);
    if (isNewsletter) {
        logger?.info({ key: cacheableKey }, 'Preparing raw media for newsletter');
        const { filePath, fileSha256, fileLength } = await getRawMediaUploadData(uploadData.media, options.mediaTypeOverride || mediaType, logger);
        const fileSha256B64 = fileSha256.toString('base64');
        const { mediaUrl, directPath } = await options.upload(filePath, {
            fileEncSha256B64: fileSha256B64,
            mediaType: mediaType,
            timeoutMs: options.mediaUploadTimeoutMs
        });
        await fs.unlink(filePath);
        const obj = WAProto.Message.fromObject({
            // todo: add more support here
            [`${mediaType}Message`]: MessageTypeProto[mediaType].fromObject({
                url: mediaUrl,
                directPath,
                fileSha256,
                fileLength,
                ...uploadData,
                media: undefined
            })
        });
        if (uploadData.ptv) {
            obj.ptvMessage = obj.videoMessage;
            delete obj.videoMessage;
        }
        if (obj.stickerMessage) {
            obj.stickerMessage.stickerSentTs = Date.now();
        }
        if (cacheableKey) {
            logger?.debug({ cacheableKey }, 'set cache');
            await options.mediaCache.set(cacheableKey, WAProto.Message.encode(obj).finish());
        }
        return obj;
    }
    const requiresDurationComputation = mediaType === 'audio' && typeof uploadData.seconds === 'undefined';
    const requiresThumbnailComputation = (mediaType === 'image' || mediaType === 'video') && typeof uploadData['jpegThumbnail'] === 'undefined';
    const requiresWaveformProcessing = mediaType === 'audio' && uploadData.ptt === true;
    const requiresAudioBackground = options.backgroundColor && mediaType === 'audio' && uploadData.ptt === true;
    const requiresOriginalForSomeProcessing = requiresDurationComputation || requiresThumbnailComputation;
    
    // ✅ LOGGING: Trace thumbnail computation requirement
    const reqTimestamp = new Date().toISOString();
    console.error(`[THUMBNAIL-DEBUG] ${reqTimestamp} | MediaType: ${mediaType}`);
    console.error(`[THUMBNAIL-DEBUG] ${reqTimestamp} | RequiresThumbnailComputation: ${requiresThumbnailComputation}`);
    console.error(`[THUMBNAIL-DEBUG] ${reqTimestamp} | HasJpegThumbnail: ${!!uploadData['jpegThumbnail']}`);
    console.error(`[THUMBNAIL-DEBUG] ${reqTimestamp} | JpegThumbnailType: ${typeof uploadData['jpegThumbnail']}`);
    console.error(`[THUMBNAIL-DEBUG] ${reqTimestamp} | RequiresOriginalForProcessing: ${requiresOriginalForSomeProcessing}`);
    const { mediaKey, encFilePath, originalFilePath, fileEncSha256, fileSha256, fileLength } = await encryptedStream(uploadData.media, options.mediaTypeOverride || mediaType, {
        logger,
        saveOriginalFileIfRequired: requiresOriginalForSomeProcessing,
        opts: options.options
    });
    const fileEncSha256B64 = fileEncSha256.toString('base64');
    const [{ mediaUrl, directPath }] = await Promise.all([
        (async () => {
            const result = await options.upload(encFilePath, {
                fileEncSha256B64,
                mediaType,
                timeoutMs: options.mediaUploadTimeoutMs
            });
            logger?.debug({ mediaType, cacheableKey }, 'uploaded media');
            return result;
        })(),
        (async () => {
            try {
                if (requiresThumbnailComputation) {
                    // ✅ DEBUG: Gunakan console.error (tidak di-disable oleh quiet-console.js) untuk debugging thumbnail generation - DETAIL LOGGING
                    const timestamp = new Date().toISOString();
                    console.error(`[THUMBNAIL-DEBUG] ${timestamp} | ACTION: Starting thumbnail computation`);
                    console.error(`[THUMBNAIL-DEBUG] ${timestamp} | MediaType: ${mediaType}`);
                    console.error(`[THUMBNAIL-DEBUG] ${timestamp} | OriginalFilePath: ${originalFilePath || 'MISSING'}`);
                    console.error(`[THUMBNAIL-DEBUG] ${timestamp} | OriginalFilePathExists: ${originalFilePath ? 'YES' : 'NO'}`);
                    console.error(`[THUMBNAIL-DEBUG] ${timestamp} | RequiresThumbnailComputation: ${requiresThumbnailComputation}`);
                    console.error(`[THUMBNAIL-DEBUG] ${timestamp} | SaveOriginalFileIfRequired: ${requiresOriginalForSomeProcessing}`);
                    
                    // ✅ FIX: Ensure originalFilePath exists before generating thumbnail
                    // ✅ For URL images, originalFilePath should be saved by encryptedStream when saveOriginalFileIfRequired=true
                    if (originalFilePath) {
                        try {
                            console.error(`[THUMBNAIL-DEBUG] ${timestamp} | ACTION: Generating thumbnail from file`);
                            console.error(`[THUMBNAIL-DEBUG] ${timestamp} | FilePath: ${originalFilePath}`);
                            const { thumbnail, originalImageDimensions } = await generateThumbnail(originalFilePath, mediaType, options);
                            if (thumbnail) {
                                const thumbSizeKB = Math.round(thumbnail.length / 1024);
                                console.error(`[THUMBNAIL-DEBUG] ${timestamp} | STATUS: Thumbnail generated successfully`);
                                console.error(`[THUMBNAIL-DEBUG] ${timestamp} | ThumbnailSize: ${thumbSizeKB}KB (${thumbnail.length} chars base64)`);
                                uploadData.jpegThumbnail = thumbnail;
                                if (!uploadData.width && originalImageDimensions) {
                                    uploadData.width = originalImageDimensions.width;
                                    uploadData.height = originalImageDimensions.height;
                                    console.error(`[THUMBNAIL-DEBUG] ${timestamp} | Dimensions: ${uploadData.width}x${uploadData.height}`);
                                    logger?.debug('set dimensions');
                                }
                                logger?.debug('generated thumbnail for URL image');
                                console.error(`[THUMBNAIL-DEBUG] ${timestamp} | STATUS: Thumbnail added to uploadData`);
                                console.error(`[THUMBNAIL-DEBUG] ${timestamp} | FINAL: Thumbnail ready for upload`);
                            } else {
                                console.error(`[THUMBNAIL-DEBUG] ${timestamp} | ERROR: Thumbnail generation returned null/undefined`);
                                console.error(`[THUMBNAIL-DEBUG] ${timestamp} | STATUS: Continuing without thumbnail`);
                            }
                        } catch (thumbError) {
                            console.error(`[THUMBNAIL-DEBUG] ${timestamp} | ERROR: Failed to generate thumbnail`);
                            console.error(`[THUMBNAIL-DEBUG] ${timestamp} | ErrorMessage: ${thumbError.message}`);
                            console.error(`[THUMBNAIL-DEBUG] ${timestamp} | ErrorStack: ${thumbError.stack}`);
                            console.error(`[THUMBNAIL-DEBUG] ${timestamp} | STATUS: Continuing without thumbnail`);
                            logger?.warn({ err: thumbError }, 'failed to generate thumbnail, continuing without thumbnail');
                        }
                    } else {
                        console.error(`[THUMBNAIL-DEBUG] ${timestamp} | ERROR: originalFilePath not available`);
                        console.error(`[THUMBNAIL-DEBUG] ${timestamp} | RequiresThumbnailComputation: ${requiresThumbnailComputation}`);
                        console.error(`[THUMBNAIL-DEBUG] ${timestamp} | SaveOriginalFileIfRequired: ${requiresOriginalForSomeProcessing}`);
                        console.error(`[THUMBNAIL-DEBUG] ${timestamp} | STATUS: Thumbnail will not be generated - originalFilePath is MISSING`);
                        logger?.warn('originalFilePath not available for thumbnail generation - thumbnail will not be generated');
                    }
                } else {
                    console.error(`⏭️ [THUMBNAIL] Skipping thumbnail computation - jpegThumbnail already exists or not required`);
                }
                if (requiresDurationComputation) {
                    uploadData.seconds = await getAudioDuration(originalFilePath);
                    logger?.debug('computed audio duration');
                }
                if (requiresWaveformProcessing) {
                    uploadData.waveform = await getAudioWaveform(originalFilePath, logger);
                    logger?.debug('processed waveform');
                }
                if (requiresAudioBackground) {
                    uploadData.backgroundArgb = await assertColor(options.backgroundColor);
                    logger?.debug('computed backgroundColor audio status');
                }
            }
            catch (error) {
                logger?.warn({ trace: error.stack }, 'failed to obtain extra info');
            }
        })()
    ]).finally(async () => {
        try {
            await fs.unlink(encFilePath);
            if (originalFilePath) {
                await fs.unlink(originalFilePath);
            }
            logger?.debug('removed tmp files');
        }
        catch (error) {
            logger?.warn('failed to remove tmp file');
        }
    });
    const obj = WAProto.Message.fromObject({
        [`${mediaType}Message`]: MessageTypeProto[mediaType].fromObject({
            url: mediaUrl,
            directPath,
            mediaKey,
            fileEncSha256,
            fileSha256,
            fileLength,
            mediaKeyTimestamp: unixTimestampSeconds(),
            ...uploadData,
            media: undefined
        })
    });
    if (uploadData.ptv) {
        obj.ptvMessage = obj.videoMessage;
        delete obj.videoMessage;
    }
    if (cacheableKey) {
        logger?.debug({ cacheableKey }, 'set cache');
        await options.mediaCache.set(cacheableKey, WAProto.Message.encode(obj).finish());
    }
    return obj;
};
export const prepareDisappearingMessageSettingContent = (ephemeralExpiration) => {
    ephemeralExpiration = ephemeralExpiration || 0;
    const content = {
        ephemeralMessage: {
            message: {
                protocolMessage: {
                    type: WAProto.Message.ProtocolMessage.Type.EPHEMERAL_SETTING,
                    ephemeralExpiration
                }
            }
        }
    };
    return WAProto.Message.fromObject(content);
};
/**
 * Generate forwarded message content like WA does
 * @param message the message to forward
 * @param options.forceForward will show the message as forwarded even if it is from you
 */
export const generateForwardMessageContent = (message, forceForward) => {
    let content = message.message;
    if (!content) {
        throw new Boom('no content in message', { statusCode: 400 });
    }
    // hacky copy
    content = normalizeMessageContent(content);
    content = proto.Message.decode(proto.Message.encode(content).finish());
    let key = Object.keys(content)[0];
    let score = content?.[key]?.contextInfo?.forwardingScore || 0;
    score += message.key.fromMe && !forceForward ? 0 : 1;
    if (key === 'conversation') {
        content.extendedTextMessage = { text: content[key] };
        delete content.conversation;
        key = 'extendedTextMessage';
    }
    const key_ = content?.[key];
    if (score > 0) {
        key_.contextInfo = { forwardingScore: score, isForwarded: true };
    }
    else {
        key_.contextInfo = {};
    }
    return content;
};
export const generateWAMessageContent = async (message, options) => {
    var _a, _b;
    let m = {};
    if ('text' in message) {
        const extContent = { text: message.text };
        let urlInfo = message.linkPreview;
        if (typeof urlInfo === 'undefined') {
            urlInfo = await generateLinkPreviewIfRequired(message.text, options.getUrlInfo, options.logger);
        }
        if (urlInfo) {
            extContent.matchedText = urlInfo['matched-text'];
            extContent.jpegThumbnail = urlInfo.jpegThumbnail;
            extContent.description = urlInfo.description;
            extContent.title = urlInfo.title;
            extContent.previewType = 0;
            const img = urlInfo.highQualityThumbnail;
            if (img) {
                extContent.thumbnailDirectPath = img.directPath;
                extContent.mediaKey = img.mediaKey;
                extContent.mediaKeyTimestamp = img.mediaKeyTimestamp;
                extContent.thumbnailWidth = img.width;
                extContent.thumbnailHeight = img.height;
                extContent.thumbnailSha256 = img.fileSha256;
                extContent.thumbnailEncSha256 = img.fileEncSha256;
            }
        }
        if (options.backgroundColor) {
            extContent.backgroundArgb = await assertColor(options.backgroundColor);
        }
        if (options.font) {
            extContent.font = options.font;
        }
        m.extendedTextMessage = extContent;
    }
    else if ('contacts' in message) {
        const contactLen = message.contacts.contacts.length;
        if (!contactLen) {
            throw new Boom('require atleast 1 contact', { statusCode: 400 });
        }
        if (contactLen === 1) {
            m.contactMessage = WAProto.Message.ContactMessage.create(message.contacts.contacts[0]);
        }
        else {
            m.contactsArrayMessage = WAProto.Message.ContactsArrayMessage.create(message.contacts);
        }
    }
    else if ('location' in message) {
        m.locationMessage = WAProto.Message.LocationMessage.create(message.location);
    }
    else if ('react' in message) {
        if (!message.react.senderTimestampMs) {
            message.react.senderTimestampMs = Date.now();
        }
        m.reactionMessage = WAProto.Message.ReactionMessage.create(message.react);
    }
    else if ('delete' in message) {
        m.protocolMessage = {
            key: message.delete,
            type: WAProto.Message.ProtocolMessage.Type.REVOKE
        };
    }
    else if ('forward' in message) {
        m = generateForwardMessageContent(message.forward, message.force);
    }
    else if ('disappearingMessagesInChat' in message) {
        const exp = typeof message.disappearingMessagesInChat === 'boolean'
            ? message.disappearingMessagesInChat
                ? WA_DEFAULT_EPHEMERAL
                : 0
            : message.disappearingMessagesInChat;
        m = prepareDisappearingMessageSettingContent(exp);
    }
    else if ('groupInvite' in message) {
        m.groupInviteMessage = {};
        m.groupInviteMessage.inviteCode = message.groupInvite.inviteCode;
        m.groupInviteMessage.inviteExpiration = message.groupInvite.inviteExpiration;
        m.groupInviteMessage.caption = message.groupInvite.text;
        m.groupInviteMessage.groupJid = message.groupInvite.jid;
        m.groupInviteMessage.groupName = message.groupInvite.subject;
        //TODO: use built-in interface and get disappearing mode info etc.
        //TODO: cache / use store!?
        if (options.getProfilePicUrl) {
            const pfpUrl = await options.getProfilePicUrl(message.groupInvite.jid, 'preview');
            if (pfpUrl) {
                const resp = await fetch(pfpUrl, { method: 'GET', dispatcher: options?.options?.dispatcher });
                if (resp.ok) {
                    const buf = Buffer.from(await resp.arrayBuffer());
                    m.groupInviteMessage.jpegThumbnail = buf;
                }
            }
        }
    }
    else if ('pin' in message) {
        m.pinInChatMessage = {};
        m.messageContextInfo = {};
        m.pinInChatMessage.key = message.pin;
        m.pinInChatMessage.type = message.type;
        m.pinInChatMessage.senderTimestampMs = Date.now();
        m.messageContextInfo.messageAddOnDurationInSecs = message.type === 1 ? message.time || 86400 : 0;
    }
    else if ('buttonReply' in message) {
        switch (message.type) {
            case 'template':
                m.templateButtonReplyMessage = {
                    selectedDisplayText: message.buttonReply.displayText,
                    selectedId: message.buttonReply.id,
                    selectedIndex: message.buttonReply.index
                };
                break;
            case 'plain':
                m.buttonsResponseMessage = {
                    selectedButtonId: message.buttonReply.id,
                    selectedDisplayText: message.buttonReply.displayText,
                    type: proto.Message.ButtonsResponseMessage.Type.DISPLAY_TEXT
                };
                break;
        }
    }
    else if ('ptv' in message && message.ptv) {
        const { videoMessage } = await prepareWAMessageMedia({ video: message.video }, options);
        m.ptvMessage = videoMessage;
    }
    else if ('product' in message) {
        const { imageMessage } = await prepareWAMessageMedia({ image: message.product.productImage }, options);
        m.productMessage = WAProto.Message.ProductMessage.create({
            ...message,
            product: {
                ...message.product,
                productImage: imageMessage
            }
        });
    }
    else if ('listReply' in message) {
        m.listResponseMessage = { ...message.listReply };
    }
    else if ('event' in message) {
        m.eventMessage = {};
        const startTime = Math.floor(message.event.startDate.getTime() / 1000);
        if (message.event.call && options.getCallLink) {
            const token = await options.getCallLink(message.event.call, { startTime });
            m.eventMessage.joinLink = (message.event.call === 'audio' ? CALL_AUDIO_PREFIX : CALL_VIDEO_PREFIX) + token;
        }
        m.messageContextInfo = {
            // encKey
            messageSecret: message.event.messageSecret || randomBytes(32)
        };
        m.eventMessage.name = message.event.name;
        m.eventMessage.description = message.event.description;
        m.eventMessage.startTime = startTime;
        m.eventMessage.endTime = message.event.endDate ? message.event.endDate.getTime() / 1000 : undefined;
        m.eventMessage.isCanceled = message.event.isCancelled ?? false;
        m.eventMessage.extraGuestsAllowed = message.event.extraGuestsAllowed;
        m.eventMessage.isScheduleCall = message.event.isScheduleCall ?? false;
        m.eventMessage.location = message.event.location;
    }
    else if ('poll' in message) {
        (_a = message.poll).selectableCount || (_a.selectableCount = 0);
        (_b = message.poll).toAnnouncementGroup || (_b.toAnnouncementGroup = false);
        if (!Array.isArray(message.poll.values)) {
            throw new Boom('Invalid poll values', { statusCode: 400 });
        }
        if (message.poll.selectableCount < 0 || message.poll.selectableCount > message.poll.values.length) {
            throw new Boom(`poll.selectableCount in poll should be >= 0 and <= ${message.poll.values.length}`, {
                statusCode: 400
            });
        }
        m.messageContextInfo = {
            // encKey
            messageSecret: message.poll.messageSecret || randomBytes(32)
        };
        const pollCreationMessage = {
            name: message.poll.name,
            selectableOptionsCount: message.poll.selectableCount,
            options: message.poll.values.map(optionName => ({ optionName }))
        };
        if (message.poll.toAnnouncementGroup) {
            // poll v2 is for community announcement groups (single select and multiple)
            m.pollCreationMessageV2 = pollCreationMessage;
        }
        else {
            if (message.poll.selectableCount === 1) {
                //poll v3 is for single select polls
                m.pollCreationMessageV3 = pollCreationMessage;
            }
            else {
                // poll for multiple choice polls
                m.pollCreationMessage = pollCreationMessage;
            }
        }
    }
    else if ('sharePhoneNumber' in message) {
        m.protocolMessage = {
            type: proto.Message.ProtocolMessage.Type.SHARE_PHONE_NUMBER
        };
    }
    else if ('requestPhoneNumber' in message) {
        m.requestPhoneNumberMessage = {};
    }
    else if ('limitSharing' in message) {
        m.protocolMessage = {
            type: proto.Message.ProtocolMessage.Type.LIMIT_SHARING,
            limitSharing: {
                sharingLimited: message.limitSharing === true,
                trigger: 1,
                limitSharingSettingTimestamp: Date.now(),
                initiatedByMe: true
            }
        };
    }
    else {
        m = await prepareWAMessageMedia(message, options);
    }
    if ('buttons' in message && !!message.buttons) {
        const buttonsMessage = {
            buttons: message.buttons.map(b => ({ ...b, type: proto.Message.ButtonsMessage.Button.Type.RESPONSE }))
        };
        if ('text' in message) {
            buttonsMessage.contentText = message.text;
            buttonsMessage.headerType = ButtonType.EMPTY;
        }
        else {
            if ('caption' in message) {
                buttonsMessage.contentText = message.caption;
            }
            const type = Object.keys(m)[0].replace('Message', '').toUpperCase();
            buttonsMessage.headerType = ButtonType[type];
            Object.assign(buttonsMessage, m);
        }
        if ('footer' in message && !!message.footer) {
            buttonsMessage.footerText = message.footer;
        }
        m = { buttonsMessage };
    }
    else if ('templateButtons' in message && !!message.templateButtons) {
        const msg = {
            hydratedButtons: message.templateButtons
        };
        if ('text' in message) {
            msg.hydratedContentText = message.text;
        }
        else {
            if ('caption' in message) {
                msg.hydratedContentText = message.caption;
            }
            Object.assign(msg, m);
        }
        if ('footer' in message && !!message.footer) {
            msg.hydratedFooterText = message.footer;
        }
        m = {
            templateMessage: {
                fourRowTemplate: msg,
                hydratedTemplate: msg
            }
        };
    }
    if ('sections' in message && !!message.sections) {
        const listMessage = {
            sections: message.sections,
            buttonText: message.buttonText,
            title: message.title,
            footerText: message.footer,
            description: message.text,
            listType: proto.Message.ListMessage.ListType.SINGLE_SELECT
        };
        m = { listMessage };
    }
    // Handle interactiveButtons (bail-lite native support for cta_url and other interactive buttons)
    // Support: text, image, video, document, location, product with interactiveButtons
    if ('interactiveButtons' in message && Array.isArray(message.interactiveButtons) && message.interactiveButtons.length > 0) {
        // ✅ LOGGING: Trace interactiveButtons handler
        const interactiveTimestamp = new Date().toISOString();
        console.error(`[THUMBNAIL-DEBUG] ${interactiveTimestamp} | ACTION: interactiveButtons handler CALLED`);
        console.error(`[THUMBNAIL-DEBUG] ${interactiveTimestamp} | MessageKeys: ${Object.keys(message).join(',')}`);
        console.error(`[THUMBNAIL-DEBUG] ${interactiveTimestamp} | MKeys: ${Object.keys(m || {}).join(',')}`);
        console.error(`[THUMBNAIL-DEBUG] ${interactiveTimestamp} | HasImageMessage: ${!!(m && m.imageMessage)}`);
        if (m && m.imageMessage) {
            console.error(`[THUMBNAIL-DEBUG] ${interactiveTimestamp} | ImageMessageHasJpegThumbnail: ${!!m.imageMessage.jpegThumbnail}`);
        }
        
        const buttons = message.interactiveButtons.map(btn => ({
            name: btn.name,
            buttonParamsJson: btn.buttonParamsJson
        }));
        
        const nativeFlowMessage = WAProto.Message.InteractiveMessage.NativeFlowMessage.create({
            buttons: buttons,
            messageParamsJson: JSON.stringify({})
        });
        
        const interactiveMessage = {
            nativeFlowMessage: nativeFlowMessage
        };
        
        // Handle body: text or caption
        if ('text' in message && message.text) {
            interactiveMessage.body = WAProto.Message.InteractiveMessage.Body.create({
                text: message.text
            });
        } else if ('caption' in message && message.caption) {
            interactiveMessage.body = WAProto.Message.InteractiveMessage.Body.create({
                text: message.caption
            });
        }
        
        // Handle header: IMPORTANT - check m FIRST before creating header
        // m should already contain imageMessage/videoMessage/etc from prepareWAMessageMedia
        // prepareWAMessageMedia returns { imageMessage: {...} } or { videoMessage: {...} } etc
        const messageType = Object.keys(m)[0] || '';
        let headerContent = {};
        
        // DEBUG: Log m structure to understand what we have
        // options.logger?.debug({ messageType, hasImageMessage: !!m.imageMessage, mKeys: Object.keys(m) }, 'interactiveButtons handler - checking m');
        
        // PRIORITY: Media must be in header for image/video/document/location/product
        // Check m.imageMessage directly (prepareWAMessageMedia returns { imageMessage: {...} })
        if (m.imageMessage) {
            console.error(`[THUMBNAIL-DEBUG] ${interactiveTimestamp} | ACTION: Copying imageMessage to header`);
            console.error(`[THUMBNAIL-DEBUG] ${interactiveTimestamp} | ImageMessageJpegThumbnail: ${!!m.imageMessage.jpegThumbnail}`);
            // Copy imageMessage to header - this is CRITICAL for image to show
            // Don't use .create() - use the object directly as it's already a proto message
            headerContent.imageMessage = m.imageMessage;
            // IMPORTANT: Set hasMediaAttachment to true when we have media
            headerContent.hasMediaAttachment = true;
        } else if (m.videoMessage) {
            headerContent.videoMessage = m.videoMessage;
            headerContent.hasMediaAttachment = true;
        } else if (m.documentMessage) {
            headerContent.documentMessage = m.documentMessage;
            headerContent.hasMediaAttachment = true;
        } else if (m.locationMessage) {
            headerContent.locationMessage = m.locationMessage;
            headerContent.hasMediaAttachment = true;
        } else if (m.productMessage) {
            headerContent.productMessage = m.productMessage;
            headerContent.hasMediaAttachment = true;
        }
        
        // Fallback: Check messageType if direct check failed
        if (Object.keys(headerContent).length === 0 || !headerContent.hasMediaAttachment) {
            if (messageType === 'imageMessage' && m.imageMessage) {
                headerContent.imageMessage = m.imageMessage;
                headerContent.hasMediaAttachment = true;
            } else if (messageType === 'videoMessage' && m.videoMessage) {
                headerContent.videoMessage = m.videoMessage;
                headerContent.hasMediaAttachment = true;
            } else if (messageType === 'documentMessage' && m.documentMessage) {
                headerContent.documentMessage = m.documentMessage;
                headerContent.hasMediaAttachment = true;
            } else if (messageType === 'locationMessage' && m.locationMessage) {
                headerContent.locationMessage = m.locationMessage;
                headerContent.hasMediaAttachment = true;
            } else if (messageType === 'productMessage' && m.productMessage) {
                headerContent.productMessage = m.productMessage;
                headerContent.hasMediaAttachment = true;
            }
        }
        
        // Add title and subtitle if provided (can be combined with media)
        // IMPORTANT: Title and subtitle can be used WITHOUT media (text-only interactive message)
        // Title and subtitle MUST be in header, even for text-only messages
        // Only add if value is truthy (not empty string, not null, not undefined)
        if ('title' in message && message.title && message.title.trim()) {
            headerContent.title = message.title.trim();
        }
        if ('subtitle' in message && message.subtitle && message.subtitle.trim()) {
            headerContent.subtitle = message.subtitle.trim();
        }
        
        // Create header if we have any content (media, title, or subtitle)
        // NOTE: Header MUST be created if we have:
        //   1. Media (image, video, document, location, product) - REQUIRED
        //   2. Title - can be standalone or with media
        //   3. Subtitle - can be standalone or with media
        // For text-only interactive messages, header is created if title/subtitle exist
        // Even if headerContent only has title/subtitle (no media), header must be created
        if (Object.keys(headerContent).length > 0) {
            interactiveMessage.header = WAProto.Message.InteractiveMessage.Header.create(headerContent);
        }
        
        // Handle footer (optional)
        // Only add if value is truthy (not empty string, not null, not undefined)
        if ('footer' in message && message.footer && message.footer.trim()) {
            interactiveMessage.footer = WAProto.Message.InteractiveMessage.Footer.create({
                text: message.footer.trim()
            });
        }
        
        // Create interactive message - this will replace m
        m = { interactiveMessage: WAProto.Message.InteractiveMessage.create(interactiveMessage) };
    }
    if ('viewOnce' in message && !!message.viewOnce) {
        m = { viewOnceMessage: { message: m } };
    }
    if ('mentions' in message && message.mentions?.length) {
        const messageType = Object.keys(m)[0];
        const key = m[messageType];
        if ('contextInfo' in key && !!key.contextInfo) {
            key.contextInfo.mentionedJid = message.mentions;
        }
        else if (key) {
            key.contextInfo = {
                mentionedJid: message.mentions
            };
        }
    }
    if ('edit' in message) {
        m = {
            protocolMessage: {
                key: message.edit,
                editedMessage: m,
                timestampMs: Date.now(),
                type: WAProto.Message.ProtocolMessage.Type.MESSAGE_EDIT
            }
        };
    }
    if ('contextInfo' in message && !!message.contextInfo) {
        const messageType = Object.keys(m)[0];
        const key = m[messageType];
        if ('contextInfo' in key && !!key.contextInfo) {
            key.contextInfo = { ...key.contextInfo, ...message.contextInfo };
        }
        else if (key) {
            key.contextInfo = message.contextInfo;
        }
    }
    return WAProto.Message.create(m);
};
export const generateWAMessageFromContent = (jid, message, options) => {
    // set timestamp to now
    // if not specified
    if (!options.timestamp) {
        options.timestamp = new Date();
    }
    const innerMessage = normalizeMessageContent(message);
    const key = getContentType(innerMessage);
    const timestamp = unixTimestampSeconds(options.timestamp);
    const { quoted, userJid } = options;
    if (quoted && !isJidNewsletter(jid)) {
        const participant = quoted.key.fromMe
            ? userJid // TODO: Add support for LIDs
            : quoted.participant || quoted.key.participant || quoted.key.remoteJid;
        let quotedMsg = normalizeMessageContent(quoted.message);
        const msgType = getContentType(quotedMsg);
        // strip any redundant properties
        quotedMsg = proto.Message.create({ [msgType]: quotedMsg[msgType] });
        const quotedContent = quotedMsg[msgType];
        if (typeof quotedContent === 'object' && quotedContent && 'contextInfo' in quotedContent) {
            delete quotedContent.contextInfo;
        }
        const contextInfo = ('contextInfo' in innerMessage[key] && innerMessage[key]?.contextInfo) || {};
        contextInfo.participant = jidNormalizedUser(participant);
        contextInfo.stanzaId = quoted.key.id;
        contextInfo.quotedMessage = quotedMsg;
        // if a participant is quoted, then it must be a group
        // hence, remoteJid of group must also be entered
        if (jid !== quoted.key.remoteJid) {
            contextInfo.remoteJid = quoted.key.remoteJid;
        }
        if (contextInfo && innerMessage[key]) {
            /* @ts-ignore */
            innerMessage[key].contextInfo = contextInfo;
        }
    }
    if (
    // if we want to send a disappearing message
    !!options?.ephemeralExpiration &&
        // and it's not a protocol message -- delete, toggle disappear message
        key !== 'protocolMessage' &&
        // already not converted to disappearing message
        key !== 'ephemeralMessage' &&
        // newsletters don't support ephemeral messages
        !isJidNewsletter(jid)) {
        /* @ts-ignore */
        innerMessage[key].contextInfo = {
            ...(innerMessage[key].contextInfo || {}),
            expiration: options.ephemeralExpiration || WA_DEFAULT_EPHEMERAL
            //ephemeralSettingTimestamp: options.ephemeralOptions.eph_setting_ts?.toString()
        };
    }
    message = WAProto.Message.create(message);
    const messageJSON = {
        key: {
            remoteJid: jid,
            fromMe: true,
            id: options?.messageId || generateMessageIDV2()
        },
        message: message,
        messageTimestamp: timestamp,
        messageStubParameters: [],
        participant: isJidGroup(jid) || isJidStatusBroadcast(jid) ? userJid : undefined, // TODO: Add support for LIDs
        status: WAMessageStatus.PENDING
    };
    return WAProto.WebMessageInfo.fromObject(messageJSON);
};
export const generateWAMessage = async (jid, content, options) => {
    // ensure msg ID is with every log
    options.logger = options?.logger?.child({ msgId: options.messageId });
    // Pass jid in the options to generateWAMessageContent
    return generateWAMessageFromContent(jid, await generateWAMessageContent(content, { ...options, jid }), options);
};
/** Get the key to access the true type of content */
export const getContentType = (content) => {
    if (content) {
        const keys = Object.keys(content);
        const key = keys.find(k => (k === 'conversation' || k.includes('Message')) && k !== 'senderKeyDistributionMessage');
        return key;
    }
};
/**
 * Normalizes ephemeral, view once messages to regular message content
 * Eg. image messages in ephemeral messages, in view once messages etc.
 * @param content
 * @returns
 */
export const normalizeMessageContent = (content) => {
    if (!content) {
        return undefined;
    }
    // set max iterations to prevent an infinite loop
    for (let i = 0; i < 5; i++) {
        const inner = getFutureProofMessage(content);
        if (!inner) {
            break;
        }
        content = inner.message;
    }
    return content;
    function getFutureProofMessage(message) {
        return (message?.ephemeralMessage ||
            message?.viewOnceMessage ||
            message?.documentWithCaptionMessage ||
            message?.viewOnceMessageV2 ||
            message?.viewOnceMessageV2Extension ||
            message?.editedMessage);
    }
};
/**
 * Extract the true message content from a message
 * Eg. extracts the inner message from a disappearing message/view once message
 */
export const extractMessageContent = (content) => {
    const extractFromTemplateMessage = (msg) => {
        if (msg.imageMessage) {
            return { imageMessage: msg.imageMessage };
        }
        else if (msg.documentMessage) {
            return { documentMessage: msg.documentMessage };
        }
        else if (msg.videoMessage) {
            return { videoMessage: msg.videoMessage };
        }
        else if (msg.locationMessage) {
            return { locationMessage: msg.locationMessage };
        }
        else {
            return {
                conversation: 'contentText' in msg ? msg.contentText : 'hydratedContentText' in msg ? msg.hydratedContentText : ''
            };
        }
    };
    content = normalizeMessageContent(content);
    if (content?.buttonsMessage) {
        return extractFromTemplateMessage(content.buttonsMessage);
    }
    if (content?.templateMessage?.hydratedFourRowTemplate) {
        return extractFromTemplateMessage(content?.templateMessage?.hydratedFourRowTemplate);
    }
    if (content?.templateMessage?.hydratedTemplate) {
        return extractFromTemplateMessage(content?.templateMessage?.hydratedTemplate);
    }
    if (content?.templateMessage?.fourRowTemplate) {
        return extractFromTemplateMessage(content?.templateMessage?.fourRowTemplate);
    }
    return content;
};
/**
 * Returns the device predicted by message ID
 */
export const getDevice = (id) => /^3A.{18}$/.test(id)
    ? 'ios'
    : /^3E.{20}$/.test(id)
        ? 'web'
        : /^(.{21}|.{32})$/.test(id)
            ? 'android'
            : /^(3F|.{18}$)/.test(id)
                ? 'desktop'
                : 'unknown';
/** Upserts a receipt in the message */
export const updateMessageWithReceipt = (msg, receipt) => {
    msg.userReceipt = msg.userReceipt || [];
    const recp = msg.userReceipt.find(m => m.userJid === receipt.userJid);
    if (recp) {
        Object.assign(recp, receipt);
    }
    else {
        msg.userReceipt.push(receipt);
    }
};
/** Update the message with a new reaction */
export const updateMessageWithReaction = (msg, reaction) => {
    const authorID = getKeyAuthor(reaction.key);
    const reactions = (msg.reactions || []).filter(r => getKeyAuthor(r.key) !== authorID);
    reaction.text = reaction.text || '';
    reactions.push(reaction);
    msg.reactions = reactions;
};
/** Update the message with a new poll update */
export const updateMessageWithPollUpdate = (msg, update) => {
    const authorID = getKeyAuthor(update.pollUpdateMessageKey);
    const reactions = (msg.pollUpdates || []).filter(r => getKeyAuthor(r.pollUpdateMessageKey) !== authorID);
    if (update.vote?.selectedOptions?.length) {
        reactions.push(update);
    }
    msg.pollUpdates = reactions;
};
/**
 * Aggregates all poll updates in a poll.
 * @param msg the poll creation message
 * @param meId your jid
 * @returns A list of options & their voters
 */
export function getAggregateVotesInPollMessage({ message, pollUpdates }, meId) {
    const opts = message?.pollCreationMessage?.options ||
        message?.pollCreationMessageV2?.options ||
        message?.pollCreationMessageV3?.options ||
        [];
    const voteHashMap = opts.reduce((acc, opt) => {
        const hash = sha256(Buffer.from(opt.optionName || '')).toString();
        acc[hash] = {
            name: opt.optionName || '',
            voters: []
        };
        return acc;
    }, {});
    for (const update of pollUpdates || []) {
        const { vote } = update;
        if (!vote) {
            continue;
        }
        for (const option of vote.selectedOptions || []) {
            const hash = option.toString();
            let data = voteHashMap[hash];
            if (!data) {
                voteHashMap[hash] = {
                    name: 'Unknown',
                    voters: []
                };
                data = voteHashMap[hash];
            }
            voteHashMap[hash].voters.push(getKeyAuthor(update.pollUpdateMessageKey, meId));
        }
    }
    return Object.values(voteHashMap);
}
/** Given a list of message keys, aggregates them by chat & sender. Useful for sending read receipts in bulk */
export const aggregateMessageKeysNotFromMe = (keys) => {
    const keyMap = {};
    for (const { remoteJid, id, participant, fromMe } of keys) {
        if (!fromMe) {
            const uqKey = `${remoteJid}:${participant || ''}`;
            if (!keyMap[uqKey]) {
                keyMap[uqKey] = {
                    jid: remoteJid,
                    participant: participant,
                    messageIds: []
                };
            }
            keyMap[uqKey].messageIds.push(id);
        }
    }
    return Object.values(keyMap);
};
const REUPLOAD_REQUIRED_STATUS = [410, 404];
/**
 * Downloads the given message. Throws an error if it's not a media message
 */
export const downloadMediaMessage = async (message, type, options, ctx) => {
    const result = await downloadMsg().catch(async (error) => {
        if (ctx &&
            typeof error?.status === 'number' && // treat errors with status as HTTP failures requiring reupload
            REUPLOAD_REQUIRED_STATUS.includes(error.status)) {
            ctx.logger.info({ key: message.key }, 'sending reupload media request...');
            // request reupload
            message = await ctx.reuploadRequest(message);
            const result = await downloadMsg();
            return result;
        }
        throw error;
    });
    return result;
    async function downloadMsg() {
        const mContent = extractMessageContent(message.message);
        if (!mContent) {
            throw new Boom('No message present', { statusCode: 400, data: message });
        }
        const contentType = getContentType(mContent);
        let mediaType = contentType?.replace('Message', '');
        const media = mContent[contentType];
        if (!media || typeof media !== 'object' || (!('url' in media) && !('thumbnailDirectPath' in media))) {
            throw new Boom(`"${contentType}" message is not a media message`);
        }
        let download;
        if ('thumbnailDirectPath' in media && !('url' in media)) {
            download = {
                directPath: media.thumbnailDirectPath,
                mediaKey: media.mediaKey
            };
            mediaType = 'thumbnail-link';
        }
        else {
            download = media;
        }
        const stream = await downloadContentFromMessage(download, mediaType, options);
        if (type === 'buffer') {
            const bufferArray = [];
            for await (const chunk of stream) {
                bufferArray.push(chunk);
            }
            return Buffer.concat(bufferArray);
        }
        return stream;
    }
};
/** Checks whether the given message is a media message; if it is returns the inner content */
export const assertMediaContent = (content) => {
    content = extractMessageContent(content);
    const mediaContent = content?.documentMessage ||
        content?.imageMessage ||
        content?.videoMessage ||
        content?.audioMessage ||
        content?.stickerMessage;
    if (!mediaContent) {
        throw new Boom('given message is not a media message', { statusCode: 400, data: content });
    }
    return mediaContent;
};
//# sourceMappingURL=messages.js.map