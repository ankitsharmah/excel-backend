const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');
const multer = require('multer');
const XLSX = require('xlsx');
const papa = require('papaparse');
const path = require('path');
const fs = require('fs');

const app = express();
const PORT = process.env.PORT || 9000;

// Middleware
app.use(cors("*"));
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));

// MongoDB Connection
mongoose.connect('mongodb+srv://as7235740:nh4ohtdJdEcpalam@cluster0.aal4w.mongodb.net/', {
  useNewUrlParser: true,
  useUnifiedTopology: true,
})
.then(() => console.log('MongoDB connected'))
.catch(err => console.error('MongoDB connection error:', err));

// Define Schema for Spreadsheets

const columnSchema = new mongoose.Schema({
    name: String,
    locked: { type: Boolean, default: false },
    type: { type: String, default: 'text' }, // text, number, date, dropdown, checkbox
    options: [String] // For dropdown type
  });
  

const spreadsheetSchema = new mongoose.Schema({
  name: { type: String, required: true },
  columns: [columnSchema],
  data: [mongoose.Schema.Types.Mixed],
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now },
});



const Spreadsheet = mongoose.model('Spreadsheet', spreadsheetSchema);

// Setup multer for file uploads
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    const uploadDir = path.join(__dirname, 'uploads');
    if (!fs.existsSync(uploadDir)) {
      fs.mkdirSync(uploadDir);
    }
    cb(null, uploadDir);
  },
  filename: (req, file, cb) => {
    cb(null, `${Date.now()}-${file.originalname}`);
  }
});

const upload = multer({ 
  storage,
  limits: { fileSize: 100 * 1024 * 1024 } // 100MB limit
});

// Routes
app.get('/api/spreadsheets/:id/download', async (req, res) => {
    try {
      const { id } = req.params;
      
      // Get spreadsheet data
      const spreadsheet = await Spreadsheet.findById(id);
      
      if (!spreadsheet) {
        return res.status(404).json({ error: 'Spreadsheet not found' });
      }
      
      // Create a new workbook
      const workbook = XLSX.utils.book_new();
      
      // Convert data to format expected by xlsx
      const headers = spreadsheet.columns.map(col => col.name);
      
      // Create worksheet data with headers as first row
      const worksheetData = [headers];
      
      // Add all the data rows
      spreadsheet.data.forEach(row => {
        const rowData = headers.map(header => row[header] || null);
        worksheetData.push(rowData);
      });
      
      // Create worksheet
      const worksheet = XLSX.utils.aoa_to_sheet(worksheetData);
      
      // Add worksheet to workbook
      XLSX.utils.book_append_sheet(workbook, worksheet, 'Sheet1');
      
      // Generate buffer
      const excelBuffer = XLSX.write(workbook, { type: 'buffer', bookType: 'xlsx' });
      
      // Set headers for file download
      res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
      res.setHeader('Content-Disposition', `attachment; filename="${spreadsheet.name}.xlsx"`);
      
      // Send the file
      res.send(excelBuffer);
    } catch (error) {
      console.error('Error downloading spreadsheet:', error);
      res.status(500).json({ error: 'Error downloading spreadsheet', details: error.message });
    }
  });
// Upload and parse file
app.post('/api/upload', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    const filePath = req.file.path;
    const fileName = req.file.originalname;
    const fileExt = path.extname(fileName).toLowerCase();
    
    let data = [];
    let columns = [];

    if (fileExt === '.xlsx' || fileExt === '.xls') {
      // Parse Excel file
      const workbook = XLSX.readFile(filePath);
      const sheetName = workbook.SheetNames[0];
      const worksheet = workbook.Sheets[sheetName];
      
      // Convert Excel to JSON
      const jsonData = XLSX.utils.sheet_to_json(worksheet, { header: 1 });
      
      // Extract headers and data
      if (jsonData.length > 0) {
        columns = jsonData[0].map(col => ({ name: col, locked: false }));
        data = jsonData.slice(1).map(row => {
          const rowData = {};
          columns.forEach((col, index) => {
            rowData[col.name] = row[index] || null;
          });
          return rowData;
        });
      }
    } else if (fileExt === '.csv') {
      // Parse CSV file
      const fileContent = fs.readFileSync(filePath, 'utf8');
      const result = papa.parse(fileContent, { header: true });
      
      if (result.data.length > 0 && result.meta.fields) {
        columns = result.meta.fields.map(field => ({ name: field, locked: false }));
        data = result.data;
      }
    } else {
      return res.status(400).json({ error: 'Unsupported file format' });
    }

    // Process in chunks if data is very large
    const chunkSize = 1000;
    const totalChunks = Math.ceil(data.length / chunkSize);
    
    // Create a new spreadsheet in MongoDB
    const spreadsheet = new Spreadsheet({
      name: fileName,
      columns,
      data: [], // Start with empty data, will be populated in chunks
    });
    
    await spreadsheet.save();
    
    // Save data in chunks to avoid MongoDB document size limits
    for (let i = 0; i < totalChunks; i++) {
      const dataChunk = data.slice(i * chunkSize, (i + 1) * chunkSize);
      await Spreadsheet.updateOne(
        { _id: spreadsheet._id },
        { $push: { data: { $each: dataChunk } } }
      );
    }
    
    // Clean up the uploaded file
    fs.unlinkSync(filePath);
    
    res.json({
      success: true,
      spreadsheetId: spreadsheet._id,
      name: fileName,
      rowCount: data.length,
      columnCount: columns.length
    });
  } catch (error) {
    console.error('Error processing file:', error);
    res.status(500).json({ error: 'Error processing file', details: error.message });
  }
});

// Get all spreadsheets
app.get('/api/spreadsheets', async (req, res) => {
  try {
    const spreadsheets = await Spreadsheet.find({}, { data: 0 });
    res.json(spreadsheets);
  } catch (error) {
    res.status(500).json({ error: 'Error fetching spreadsheets' });
  }
});

// Get spreadsheet by ID - no pagination, returns all data
app.get('/api/spreadsheets/:id', async (req, res) => {
  try {
    const { id } = req.params;
    
    // Get the complete spreadsheet with all data
    const spreadsheet = await Spreadsheet.findById(id);
    
    if (!spreadsheet) {
      return res.status(404).json({ error: 'Spreadsheet not found' });
    }
    
    res.json(spreadsheet);
  } catch (error) {
    console.error('Error fetching spreadsheet:', error);
    res.status(500).json({ error: 'Error fetching spreadsheet', details: error.message });
  }
});

// Update cell data (batch update)
app.put('/api/spreadsheets/:id/data', async (req, res) => {
  try {
    const { id } = req.params;
    const { updates } = req.body; // Array of { rowIndex, field, value }

    if (!Array.isArray(updates)) {
      return res.status(400).json({ error: 'Updates must be an array' });
    }

    const spreadsheet = await Spreadsheet.findById(id);

    if (!spreadsheet) {
      return res.status(404).json({ error: 'Spreadsheet not found' });
    }

    // Check if trying to update locked columns
    for (const update of updates) {
      const column = spreadsheet.columns.find(col => col.name === update.field);
      if (column && column.locked) {
        return res.status(403).json({
          error: 'Cannot update locked column',
          column: update.field
        });
      }
    }

    // Apply updates to the data array
    for (const update of updates) {
      if (spreadsheet.data[update.rowIndex]) {
        spreadsheet.data[update.rowIndex][update.field] = update.value;
      }
    }

    // Mark data field as modified
    spreadsheet.markModified('data');

    spreadsheet.updatedAt = new Date();
    const savedSpreadsheet = await spreadsheet.save();

    res.json({ success: true, spreadsheet: savedSpreadsheet });
  } catch (error) {
    console.error('Error updating data:', error);
    res.status(500).json({ error: 'Error updating data', details: error.message });
  }
});
  
// Lock/Unlock column
app.put('/api/spreadsheets/:id/columns/:columnName/lock', async (req, res) => {
  try {
    const { id, columnName } = req.params;
    const { locked } = req.body;
    
    const spreadsheet = await Spreadsheet.findById(id);
    
    if (!spreadsheet) {
      return res.status(404).json({ error: 'Spreadsheet not found' });
    }
    
    const columnIndex = spreadsheet.columns.findIndex(col => col.name === columnName);
    
    if (columnIndex === -1) {
      return res.status(404).json({ error: 'Column not found' });
    }
    
    spreadsheet.columns[columnIndex].locked = locked;
    spreadsheet.updatedAt = new Date();
    await spreadsheet.save();
    
    res.json({ success: true, column: spreadsheet.columns[columnIndex] });
  } catch (error) {
    console.error('Error updating column lock status:', error);
    res.status(500).json({ error: 'Error updating column lock status', details: error.message });
  }
});

// Add new row
app.post('/api/spreadsheets/:id/row', async (req, res) => {
  try {
    const { id } = req.params;
    const rowData = req.body;
    
    const spreadsheet = await Spreadsheet.findById(id);
    
    if (!spreadsheet) {
      return res.status(404).json({ error: 'Spreadsheet not found' });
    }
    
    spreadsheet.data.push(rowData);
    spreadsheet.updatedAt = new Date();
    await spreadsheet.save();
    
    res.json({ 
      success: true, 
      rowIndex: spreadsheet.data.length - 1,
      row: rowData
    });
  } catch (error) {
    console.error('Error adding row:', error);
    res.status(500).json({ error: 'Error adding row', details: error.message });
  }
});

// Add new column - with explicit spreadsheet ID validation
app.post('/api/spreadsheets/:id/column', async (req, res) => {
    try {
      const { id } = req.params;
      const { name, defaultValue = null, type = 'text', options = [] } = req.body;
      
      // Ensure we're working with a valid ObjectId
      if (!mongoose.Types.ObjectId.isValid(id)) {
        return res.status(400).json({ error: 'Invalid spreadsheet ID format' });
      }
      
      // Explicitly fetch by the exact ID to prevent any issues
      const spreadsheet = await Spreadsheet.findOne({ _id: id });
      
      if (!spreadsheet) {
        return res.status(404).json({ error: 'Spreadsheet not found', id });
      }
      
      // Check for duplicate column name
      if (spreadsheet.columns.some(col => col.name === name)) {
        return res.status(400).json({ error: 'Column name already exists' });
      }
      
      // Create column object based on type
      const columnObject = {
        name,
        locked: false,
        type
      };
      
      // Add options if it's a dropdown
      if (type === 'dropdown' && Array.isArray(options) && options.length > 0) {
        columnObject.options = options;
      }
      
      // Add the new column to schema
      spreadsheet.columns.push(columnObject);
      
      // Add the new field to all existing data rows
      spreadsheet.data.forEach(row => {
        row[name] = defaultValue;
      });
      
      // Mark both columns and data as modified since we've updated both
      spreadsheet.markModified('columns');
      spreadsheet.markModified('data');
      
      spreadsheet.updatedAt = new Date();
      await spreadsheet.save();
      
      res.json({ 
        success: true, 
        spreadsheetId: spreadsheet._id,
        column: columnObject
      });
    } catch (error) {
      console.error('Error adding column:', error);
      res.status(500).json({ error: 'Error adding column', details: error.message });
    }
  });

// Delete a spreadsheet
app.delete('/api/spreadsheets/:id', async (req, res) => {
  try {
    const { id } = req.params;
    
    const result = await Spreadsheet.findByIdAndDelete(id);
    
    if (!result) {
      return res.status(404).json({ error: 'Spreadsheet not found' });
    }
    
    res.json({ success: true });
  } catch (error) {
    console.error('Error deleting spreadsheet:', error);
    res.status(500).json({ error: 'Error deleting spreadsheet', details: error.message });
  }
});

// Start the server
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

module.exports = app;