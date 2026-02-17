const express = require("express");
const { createClient } = require("@supabase/supabase-js");
const { PDFDocument } = require("pdf-lib");
const ftp = require("basic-ftp");
const { Readable } = require("stream");

const app = express();
app.use(express.json({ limit: "200mb" }));

const PORT = process.env.PORT || 3000;
const MERGE_API_SECRET = process.env.MERGE_API_SECRET;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

// Health check
app.get("/", (req, res) => {
  res.json({ status: "ok", service: "momentli-pdf-merge", version: "2.1.0" });
});

// Auth middleware
function checkAuth(req, res, next) {
  const secret = req.headers["x-api-secret"];
  if (!secret || secret !== MERGE_API_SECRET) {
    return res.status(401).json({ error: "Unauthorized" });
  }
  next();
}

// ========== MERGE ONLY (legacy) ==========
app.post("/merge", checkAuth, async (req, res) => {
  const { storagePaths, orderId, format } = req.body;

  if (!storagePaths || !Array.isArray(storagePaths) || storagePaths.length === 0) {
    return res.status(400).json({ error: "storagePaths is required" });
  }

  console.log(`[merge] orderId=${orderId}, format=${format}, batches=${storagePaths.length}`);

  try {
    const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
    const { mergedBytes, pageCount } = await mergeBatches(supabase, storagePaths);

    const outputPath = `temp/${orderId}/merged_${format}.pdf`;
    const { error: uploadError } = await supabase.storage
      .from("print-queue")
      .upload(outputPath, mergedBytes, { contentType: "application/pdf", upsert: true });

    if (uploadError) throw new Error(`Upload failed: ${uploadError.message}`);

    console.log(`[merge] Done: ${outputPath}, ${mergedBytes.length} bytes, ${pageCount} pages`);
    res.json({ storagePath: outputPath, size: mergedBytes.length, pages: pageCount });
  } catch (err) {
    console.error("[merge] Failed:", err);
    res.status(500).json({ error: err.message || String(err) });
  }
});

// ========== MERGE + FTP UPLOAD ==========
app.post("/merge-and-upload", checkAuth, async (req, res) => {
  const { storagePaths, orderId, format, ftp: ftpConfig, xmlContent, xmlFilename, pdfFilename } = req.body;

  if (!storagePaths || !Array.isArray(storagePaths) || storagePaths.length === 0) {
    return res.status(400).json({ error: "storagePaths is required" });
  }
  if (!ftpConfig || !ftpConfig.host || !ftpConfig.user || !ftpConfig.password) {
    return res.status(400).json({ error: "ftp config (host, user, password) is required" });
  }
  if (!pdfFilename) {
    return res.status(400).json({ error: "pdfFilename is required" });
  }

  console.log(`[merge-and-upload] orderId=${orderId}, format=${format}, batches=${storagePaths.length}, pdf=${pdfFilename}`);

  try {
    const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

    // Step 1: Merge PDFs
    const { mergedBytes, pageCount } = await mergeBatches(supabase, storagePaths);
    console.log(`[merge-and-upload] Merged: ${mergedBytes.length} bytes, ${pageCount} pages`);

    // Step 2: FTP upload
    console.log(`[merge-and-upload] Connecting to FTP: ${ftpConfig.host}`);
    const client = new ftp.Client();
    client.ftp.verbose = false;

    try {
      await client.access({
        host: ftpConfig.host,
        user: ftpConfig.user,
        password: ftpConfig.password,
        secure: false,
      });

      // Upload PDF
      console.log(`[merge-and-upload] Uploading PDF: ${pdfFilename} (${mergedBytes.length} bytes)`);
      await client.uploadFrom(Readable.from(Buffer.from(mergedBytes)), pdfFilename);
      console.log(`[merge-and-upload] PDF uploaded: ${pdfFilename}`);

      // Upload XML if provided
      let xmlUploaded = false;
      if (xmlContent && xmlFilename) {
        console.log(`[merge-and-upload] Uploading XML: ${xmlFilename}`);
        await client.uploadFrom(Readable.from(Buffer.from(xmlContent, "utf-8")), xmlFilename);
        xmlUploaded = true;
        console.log(`[merge-and-upload] XML uploaded: ${xmlFilename}`);
      }

      client.close();
      console.log(`[merge-and-upload] FTP complete`);

      // Step 3: Clean up batch files
      console.log(`[merge-and-upload] Cleaning up ${storagePaths.length} batch files`);
      try {
        await supabase.storage.from("print-queue").remove(storagePaths);
      } catch (cleanupErr) {
        console.warn("[merge-and-upload] Cleanup warning:", cleanupErr.message);
      }

      res.json({
        size: mergedBytes.length,
        pages: pageCount,
        ftpUploaded: true,
        pdfFilename,
        xmlFilename: xmlUploaded ? xmlFilename : null,
      });

    } catch (ftpErr) {
      client.close();
      throw new Error(`FTP upload failed: ${ftpErr.message}`);
    }

  } catch (err) {
    console.error("[merge-and-upload] Failed:", err);
    res.status(500).json({ error: err.message || String(err) });
  }
});

// ========== Shared helpers ==========

async function mergeBatches(supabase, storagePaths) {
  console.log(`Starting incremental merge of ${storagePaths.length} batches...`);

  const firstBuffer = await downloadBatch(supabase, storagePaths[0]);
  // NOTE: Do NOT call buffer.fill(0) — pdf-lib still references this memory for image data
  let mergedPdf = await PDFDocument.load(firstBuffer);
  console.log(`Base batch: ${mergedPdf.getPageCount()} pages`);

  for (let i = 1; i < storagePaths.length; i++) {
    console.log(`Merging batch ${i + 1}/${storagePaths.length}`);
    const batchBuffer = await downloadBatch(supabase, storagePaths[i]);
    // NOTE: Do NOT call buffer.fill(0) — pdf-lib still references this memory for image data
    const batchPdf = await PDFDocument.load(batchBuffer);

    const pages = await mergedPdf.copyPages(batchPdf, batchPdf.getPageIndices());
    for (const page of pages) {
      mergedPdf.addPage(page);
    }
    console.log(`After batch ${i + 1}: ${mergedPdf.getPageCount()} pages`);
  }

  console.log("Saving merged PDF...");
  const mergedBytes = await mergedPdf.save();
  const pageCount = mergedPdf.getPageCount();
  mergedPdf = null;

  return { mergedBytes, pageCount };
}

async function downloadBatch(supabase, storagePath) {
  const { data, error } = await supabase.storage
    .from("print-queue")
    .download(storagePath);

  if (error || !data) {
    throw new Error(`Failed to download ${storagePath}: ${error?.message}`);
  }

  return Buffer.from(await data.arrayBuffer());
}

app.listen(PORT, () => {
  console.log(`PDF merge+upload service v2.1 running on port ${PORT}`);
});
