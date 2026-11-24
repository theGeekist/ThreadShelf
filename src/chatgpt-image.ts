import * as yauzl from 'yauzl';

const MAX_IMAGE_BYTES = 8 * 1024 * 1024;

const checksum = (buffer: Buffer): number => {
  let crc = 0xffffffff;
  for (const byte of buffer) {
    crc ^= byte;
    for (let i = 0; i < 8; i++) crc = (crc >>> 1) ^ (-(crc & 1) & 0xedb88320);
  }
  return (crc ^ 0xffffffff) >>> 0;
};

const imageType = (data: Buffer): string => {
  if (data.subarray(0, 8).equals(Buffer.from([137, 80, 78, 71, 13, 10, 26, 10])))
    return 'image/png';
  if (data.length >= 3 && data[0] === 255 && data[1] === 216 && data[2] === 255)
    return 'image/jpeg';
  if (
    data.subarray(0, 6).toString('ascii') === 'GIF87a' ||
    data.subarray(0, 6).toString('ascii') === 'GIF89a'
  )
    return 'image/gif';
  if (
    data.subarray(0, 4).toString('ascii') === 'RIFF' &&
    data.subarray(8, 12).toString('ascii') === 'WEBP'
  )
    return 'image/webp';
  throw new Error('Attachment is not a supported image');
};

/** Read one uniquely named image from a configured export ZIP without extracting files. */
export const readChatGptImage = async (
  zipPath: string,
  assetId: string,
): Promise<{ data: Buffer; mimeType: string }> => {
  if (!/^file-[A-Za-z0-9_-]{1,240}$/.test(assetId)) throw new Error('Invalid attachment ID');
  const zip = await new Promise<yauzl.ZipFile>((resolve, reject) => {
    yauzl.open(
      zipPath,
      { lazyEntries: true, autoClose: false, validateEntrySizes: true, strictFileNames: true },
      (error, file) => (error ? reject(error) : resolve(file)),
    );
  });
  try {
    const entry = await new Promise<yauzl.Entry>((resolve, reject) => {
      const matches: yauzl.Entry[] = [];
      zip.on('error', reject);
      zip.on('entry', (item: yauzl.Entry) => {
        const name = item.fileName;
        if (!name.includes('/') && !name.includes('\\') && name === `${assetId}.dat`)
          matches.push(item);
        zip.readEntry();
      });
      zip.on('end', () =>
        matches.length === 1
          ? resolve(matches[0]!)
          : reject(
              new Error(
                matches.length
                  ? 'Attachment has ambiguous ZIP entries'
                  : 'Attachment is not in the export ZIP',
              ),
            ),
      );
      zip.readEntry();
    });
    if (entry.isEncrypted() || entry.uncompressedSize > MAX_IMAGE_BYTES)
      throw new Error('Attachment is encrypted or exceeds the 8 MiB limit');
    const stream = await new Promise<NodeJS.ReadableStream>((resolve, reject) =>
      zip.openReadStream(entry, (error, output) => (error ? reject(error) : resolve(output))),
    );
    const chunks: Buffer[] = [];
    let size = 0;
    for await (const part of stream) {
      const chunk = Buffer.from(part as Uint8Array);
      size += chunk.length;
      if (size > MAX_IMAGE_BYTES) throw new Error('Attachment exceeds the 8 MiB limit');
      chunks.push(chunk);
    }
    const data = Buffer.concat(chunks);
    if (data.length !== entry.uncompressedSize || checksum(data) !== entry.crc32)
      throw new Error('Attachment failed integrity verification');
    return { data, mimeType: imageType(data) };
  } finally {
    zip.close();
  }
};
