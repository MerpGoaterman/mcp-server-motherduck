// MCP MotherDuck Serverless API (secured)
import { startMotherDuckMCP } from '../src/index.js'; // direct import from source

export default async function handler(req, res) {
  try {
    // Bearer token auth
    const authHeader = req.headers['authorization'] || '';
    const token = authHeader.replace('Bearer ', '').trim();
    if (!token || token !== process.env.API_BEARER_TOKEN) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    // Check for MotherDuck API key
    if (!process.env.MOTHERDUCK_API_KEY) {
      return res.status(500).json({ error: 'Missing MOTHERDUCK_API_KEY env var' });
    }

    // Start MCP
    await startMotherDuckMCP({
      req,
      res,
      token: process.env.MOTHERDUCK_API_KEY,
      database: process.env.MOTHERDUCK_DATABASE || 'default'
    });
  } catch (err) {
    console.error('MotherDuck MCP Error:', err);
    res.status(500).json({ error: 'Internal MCP Server Error', details: err.message });
  }
}
