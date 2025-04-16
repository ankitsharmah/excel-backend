const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');
const multer = require('multer');
const XLSX = require('xlsx');
const papa = require('papaparse');
const path = require('path');
const fs = require('fs');
const http = require('http'); // Required for Socket.io
const socketIo = require('socket.io'); // Add Socket.io

const app = express();
const PORT = process.env.PORT || 9000;

// Create an HTTP server using Express app
const server = http.createServer(app);

// Initialize Socket.io with CORS settings
const io = socketIo(server, {
  cors: {
    origin: "*", // Allow all origins
    methods: ["GET", "POST", "PUT", "DELETE"],
    allowedHeaders: ["Content-Type"]
  }
});

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

// Helper function to retry operations that may encounter version conflicts
async function retryOperation(operation, maxRetries = 3) {
  let retries = 0;
  while (retries < maxRetries) {
    try {
      return await operation();
    } catch (error) {
      if (error.name === 'VersionError' && retries < maxRetries - 1) {
        retries++;
        await new Promise(resolve => setTimeout(resolve, 100)); // Small delay before retry
        continue;
      }
      throw error;
    }
  }
}

// Socket.io connection handler
io.on('connection', (socket) => {
  console.log(`Client connected: ${socket.id}`);
  
  // Listen for room joining (based on spreadsheet ID)
  socket.on('joinSpreadsheet', (spreadsheetId) => {
    socket.join(spreadsheetId);
    console.log(`Client ${socket.id} joined spreadsheet: ${spreadsheetId}`);
  });
  socket.on("cellUpdate", ({ spreadsheetId, updatedCell }) => {
    socket.to(spreadsheetId).emit("cellUpdated", updatedCell);
  });

// Cell editing start
socket.on("cellEditing", (data) => {
  console.log(`Client ${socket.id} emitted cellEditing:`, data);
  
  // Destructure with defaults to prevent errors
  const { spreadsheetId, cell = {}, user = {} } = data;
  
  if (!spreadsheetId) {
    console.error('Missing spreadsheetId in cellEditing event');
    return;
  }
  
  // console.log(`Broadcasting cellEditing to room ${spreadsheetId}`);
  socket.to(spreadsheetId).emit("cellEditing", {
    cell,
    user,
    timestamp: new Date(),
  });
});

// Cell editing stop - add detailed logging
socket.on("cellEditingStopped", (data) => {
  console.log(`Client ${socket.id} emitted cellEditingStopped:`, data);
  
  // Destructure with defaults to prevent errors
  const { spreadsheetId, cell = {}, user = {} } = data;
  
  if (!spreadsheetId) {
    console.error('Missing spreadsheetId in cellEditingStopped event');
    return;
  }
  
  console.log(`Broadcasting cellEditingStopped to room ${spreadsheetId}`);
  socket.to(spreadsheetId).emit("cellEditingStopped", {
    cell,
    user,
  });
});

  // Handle disconnection
  socket.on('disconnect', () => {
    console.log(`Client disconnected: ${socket.id}`);
  });
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

// Update cell data (batch update) - FIXED VERSION with Socket.io notifications
app.put('/api/spreadsheets/:id/data', async (req, res) => {
  try {
    const { id } = req.params;
    const { updates } = req.body; // Array of { rowIndex, field, value }

    if (!Array.isArray(updates)) {
      return res.status(400).json({ error: 'Updates must be an array' });
    }

    // First, get the spreadsheet to check locked columns
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

    // Process updates using MongoDB's atomic operators
    const bulkOperations = updates.map(update => {
      const updatePath = `data.${update.rowIndex}.${update.field}`;
      return {
        updateOne: {
          filter: { _id: id },
          update: { 
            $set: { [updatePath]: update.value, updatedAt: new Date() }
          }
        }
      };
    });

    // Execute the bulk operations
    await Spreadsheet.bulkWrite(bulkOperations);

    // Get the updated document
    const updatedSpreadsheet = await Spreadsheet.findById(id);
    
    // Emit socket event to all clients viewing this spreadsheet
    io.to(id).emit('cellUpdates', {
      updates,
      timestamp: new Date()
    });
      // onBlur={() => {
    //     io.to(id).emit('cellEditingStopped', {
    //   spreadsheetId: id,
    //   cell: { rowIndex, columnKey: field }
    // });
  

    res.json({ success: true, spreadsheet: updatedSpreadsheet });
  } catch (error) {
    console.error('Error updating data:', error);
    
    // Specific handling for version errors
    if (error.name === 'VersionError') {
      return res.status(409).json({ 
        error: 'Conflict detected, the document was modified by another request',
        details: 'Try again with the latest version of the data',
        code: 'VERSION_CONFLICT'
      });
    }
    
    res.status(500).json({ error: 'Error updating data', details: error.message });
  }
});
  
// Lock/Unlock column with Socket.io notification
app.put('/api/spreadsheets/:id/columns/:columnName/lock', async (req, res) => {
  try {
    const { id, columnName } = req.params;
    const { locked } = req.body;
    
    // Use findOneAndUpdate to avoid version conflicts
    const result = await Spreadsheet.findOneAndUpdate(
      { 
        _id: id,
        'columns.name': columnName 
      },
      { 
        $set: { 
          'columns.$.locked': locked,
          updatedAt: new Date()
        } 
      },
      { new: true }
    );
    
    if (!result) {
      return res.status(404).json({ error: 'Spreadsheet or column not found' });
    }
    
    const updatedColumn = result.columns.find(col => col.name === columnName);
    
    // Emit socket event for column lock change
    io.to(id).emit('columnLockChanged', {
      columnName,
      locked,
      timestamp: new Date()
    });
    
    res.json({ success: true, column: updatedColumn });
  } catch (error) {
    console.error('Error updating column lock status:', error);
    res.status(500).json({ error: 'Error updating column lock status', details: error.message });
  }
});

// Add new row with Socket.io notification
app.post('/api/spreadsheets/:id/row', async (req, res) => {
  try {
    const { id } = req.params;
    const rowData = req.body;
    
    // Use findOneAndUpdate to avoid version conflicts
    const result = await Spreadsheet.findByIdAndUpdate(
      id,
      { 
        $push: { data: rowData },
        $set: { updatedAt: new Date() }
      },
      { new: true }
    );
    
    if (!result) {
      return res.status(404).json({ error: 'Spreadsheet not found' });
    }
    
    const newRowIndex = result.data.length - 1;
    
    // Emit socket event for new row
    io.to(id).emit('rowAdded', {
      rowIndex: newRowIndex,
      rowData,
      timestamp: new Date()
    });
    
    res.json({ 
      success: true, 
      rowIndex: newRowIndex,
      row: rowData
    });
  } catch (error) {
    console.error('Error adding row:', error);
    res.status(500).json({ error: 'Error adding row', details: error.message });
  }
});

// Add new column with Socket.io notification
app.post('/api/spreadsheets/:id/column', async (req, res) => {
  console.log("im called");
  try {
    const { id } = req.params;
    const { name, defaultValue = null, type = 'text', options = [] } = req.body;
    console.log("object");

    if (!mongoose.Types.ObjectId.isValid(id)) {
      return res.status(400).json({ error: 'Invalid spreadsheet ID format' });
    }

    const existingSpreadsheet = await Spreadsheet.findById(id);

    if (!existingSpreadsheet) {
      return res.status(404).json({ error: 'Spreadsheet not found', id });
    }

    if (existingSpreadsheet.columns.some(col => col.name === name)) {
      return res.status(400).json({ error: 'Column name already exists' });
    }

    const columnObject = {
      name,
      locked: false,
      type
    };

    if (type === 'dropdown' && Array.isArray(options) && options.length > 0) {
      columnObject.options = options;
    }

    // Push the new column first
    await Spreadsheet.findByIdAndUpdate(
      id,
      {
        $push: { columns: columnObject },
        $set: { updatedAt: new Date() }
      }
    );
    console.log("column added");

    // Re-fetch spreadsheet to get current row count
    const freshSpreadsheet = await Spreadsheet.findById(id);

    // Build a single $set operation to update all rows
    const defaultUpdates = {};
    freshSpreadsheet.data.forEach((_, index) => {
      defaultUpdates[`data.${index}.${name}`] = defaultValue;
    });

    await Spreadsheet.updateOne(
      { _id: id },
      { $set: defaultUpdates }
    );
    console.log("default values set");

    const updatedSpreadsheet = await Spreadsheet.findById(id);
    const addedColumn = updatedSpreadsheet.columns.find(col => col.name === name);

    io.to(id).emit('columnAdded', {
      column: addedColumn,
      defaultValue,
      timestamp: new Date()
    });

    res.status(200).json({
      success: true,
      spreadsheetId: updatedSpreadsheet._id,
      column: addedColumn
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
    
    // No need to emit an event here as clients won't be viewing this spreadsheet anymore
    
    res.json({ success: true });
  } catch (error) {
    console.error('Error deleting spreadsheet:', error);
    res.status(500).json({ error: 'Error deleting spreadsheet', details: error.message });
  }
});

// Start the server using http server instead of Express app
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

module.exports = { app, server, io }; 
