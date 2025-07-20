import { startMotherDuckMCP } from 'mcp-server-motherduck';

export default async function handler(req, res) {
  try {
    await startMotherDuckMCP({
      req,
      res,
      token: process.env.MOTHERDUCK_API_KEY,
      database: process.env.MOTHERDUCK_DATABASE || 'default',
    });
  } catch (err) {
    console.error('MotherDuck MCP Error:', err);
    res.status(500).json({ error: 'Internal MCP Server Error' });
  }
}
