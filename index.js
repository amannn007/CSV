const express = require('express');
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const axios = require('axios');
const sharp = require('sharp');
const mongoose = require('mongoose');
const { v4: uuidv4 } = require('uuid');

// Connect to MongoDB
mongoose.connect('mongodb://localhost:27017/imageProcessorDB', { useNewUrlParser: true, useUnifiedTopology: true })
    .then(() => console.log('Connected to MongoDB'))
    .catch(err => console.error('Could not connect to MongoDB...', err));

// Define a Mongoose schema for Product and Image
const productSchema = new mongoose.Schema({
    productName: String,
    images: [
        {
            originalUrl: String,
            compressedUrl: String,
            imageName: String
        }
    ]
});

// Define a Mongoose schema for request processing
const requestSchema = new mongoose.Schema({
    requestId: String,
    status: String
});

const Product = mongoose.model('Product', productSchema);
const RequestStatus = mongoose.model('RequestStatus', requestSchema);

// Create the output directory if it doesn't exist
const outputDir = path.join(__dirname, 'processed_images');
if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir);
}

// Initialize Express app
const app = express();
app.use(express.json());

// Function to download images
async function downloadImage(url, outputPath) {
    const writer = fs.createWriteStream(outputPath);
    const response = await axios({
        url,
        method: 'GET',
        responseType: 'stream',
    });

    response.data.pipe(writer);

    return new Promise((resolve, reject) => {
        writer.on('finish', resolve);
        writer.on('error', reject);
    });
}

// Function to compress and save the image
async function processImage(inputPath, outputPath) {
    try {
        await sharp(inputPath)
            .jpeg({ quality: 50 }) // Compress to 50% quality
            .toFile(outputPath);
        console.log(`Image processed: ${outputPath}`);
    } catch (error) {
        console.error(`Error processing image ${outputPath}:`, error);
    }
}

// Save product and image details to the database
async function saveToDatabase(productName, images) {
    const product = new Product({
        productName: productName,
        images: images
    });
    await product.save();
    console.log(`Saved product ${productName} to database.`);
}

// Process the CSV file
async function processCsv(filePath, requestId) {
    fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', async (data) => {
            const productName = data['Product Name'].replace(/\s/g, '_'); // Replace spaces with underscores
            const urls = data['Input Image Urls'].split(',');
            let images = [];

            console.log(`Processing product: ${productName} with request ID: ${requestId}`);

            for (let i = 0; i < urls.length; i++) {
                const url = urls[i].trim();
                const inputFileName = `${productName}_image${i + 1}.jpg`;
                const outputFileName = `${productName}_image${i + 1}_compressed.jpg`;
                const inputPath = path.join(outputDir, inputFileName);
                const outputPath = path.join(outputDir, outputFileName);

                try {
                    // Download image
                    await downloadImage(url, inputPath);
                    console.log(`Downloaded image from ${url} to ${inputPath}`);

                    // Process image
                    await processImage(inputPath, outputPath);

                    // Save image info to array for database
                    images.push({
                        originalUrl: url,
                        compressedUrl: outputPath,
                        imageName: inputFileName
                    });

                } catch (error) {
                    console.error(`Error processing image from URL: ${url}`, error);
                }
            }

            // Save product and its image details to the database
            if (images.length > 0) {
                await saveToDatabase(productName, images);
            }
        })
        .on('end', async () => {
            console.log(`CSV file processing completed for request ID: ${requestId}`);
            // Update the status to completed
            await RequestStatus.updateOne({ requestId }, { status: 'completed' });
            console.log(`Updated status for request ID ${requestId} to 'completed'`);
        });
}

// API to submit a CSV file
app.post('/submit', async (req, res) => {
    const requestId = uuidv4(); // Generate a unique request ID
    const csvFilePath = path.join(__dirname, 'products.csv'); 

    // Create a new request status in the database
    const requestStatus = new RequestStatus({
        requestId: requestId,
        status: 'pending'
    });
    await requestStatus.save();
    console.log(`New request created with ID: ${requestId} and status: 'pending'`);

    // Process the CSV
    processCsv(csvFilePath, requestId);

    // Respond with the request ID
    res.status(202).send({ requestId: requestId });
});

// API to check processing status
app.get('/status/:requestId', async (req, res) => {
    const { requestId } = req.params;
    const status = await RequestStatus.findOne({ requestId });

    if (status) {
        console.log(`Status checked for request ID: ${requestId}, current status: ${status.status}`);
        res.send({ requestId: requestId, status: status.status });
    } else {
        console.log(`Request ID ${requestId} not found`);
        res.status(404).send({ message: 'Request ID not found' });
    }
});

// Start the server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});
