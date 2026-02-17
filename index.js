const express = require("express");
const { createClient } = require("@supabase/supabase-js");
const { PDFDocument } = require("pdf-lib");

const app = express();
app.use(express.json({ limit: "200mb" }));

const PORT = process.env.PORT || 3000;
const MERGE_API_SECRET = process.env.MERGE_API_SECRET;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

// Health check
app.get("/", (req, res) => {
  res.json({ status: "ok", service: "momentli-pdf-merge" });
});

app.post("/merge", async (req, res) => {
  // Auth check
  const secret = req.headers["x-api-secret"];
  if (!secret || secret !== MERGE_API_SECRET) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  const { storagePaths, orderId, format, orderNumber } = req.body;

  if (!storagePaths || !Array.isArray(storagePaths) || storagePaths.length === 0) {
    return res.status(400).json({ error: "storagePaths is required and must be a non-empty array" });
  }

  console.log(`Merge request: orderId=${orderId}, format=${format}, batches=${storagePaths.length}`);

  try {
    const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

    // INKREMENTELL MERGE: Ladda en batch i taget, aldrig mer än 2 PDFer i minnet
    console.log("Starting incremental PDF merge...");

    // Ladda batch 0 som bas
    console.log(`Loading base batch 1/${storagePaths.length}: ${storagePaths[0]}`);
    const firstBuffer = await downloadBatch(supabase, storagePaths[0]);
    let mergedPdf = await PDFDocument.load(firstBuffer);
    firstBuffer.fill(0); // Frigör minne
    console.log(`Base batch loaded: ${mergedPdf.getPageCount()} pages`);

    // Merga in resten en i taget
    for (let i = 1; i < storagePaths.length; i++) {
      console.log(`Merging batch ${i + 1}/${storagePaths.length}: ${storagePaths[i]}`);

      const batchBuffer = await downloadBatch(supabase, storagePaths[i]);
      const batchPdf = await PDFDocument.load(batchBuffer);
      batchBuffer.fill(0); // Frigör minne direkt efter load

      const pages = await mergedPdf.copyPages(batchPdf, batchPdf.getPageIndices());
      for (const page of pages) {
        mergedPdf.addPage(page);
      }

      console.log(`After batch ${i + 1}: ${mergedPdf.getPageCount()} pages total`);
    }

    // Spara slutresultatet
    console.log("Saving merged PDF...");
    const mergedBytes = await mergedPdf.save();
    const pageCount = mergedPdf.getPageCount();
    mergedPdf = null; // Frigör minne

    console.log(`Merge complete: ${mergedBytes.length} bytes, ${pageCount} pages`);

    // Ladda upp till Storage
    const outputPath = `temp/${orderId}/merged_${format}.pdf`;
    const { error: uploadError } = await supabase.storage
      .from("print-queue")
      .upload(outputPath, mergedBytes, {
        contentType: "application/pdf",
        upsert: true,
      });

    if (uploadError) {
      throw new Error(`Failed to upload merged PDF: ${uploadError.message}`);
    }

    console.log(`Uploaded merged PDF to: ${outputPath}`);

    res.json({
      storagePath: outputPath,
      size: mergedBytes.length,
      pages: pageCount,
    });

  } catch (err) {
    console.error("Merge failed:", err);
    res.status(500).json({
      error: err.message || String(err),
    });
  }
});

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
  console.log(`PDF merge service running on port ${PORT}`);
});
