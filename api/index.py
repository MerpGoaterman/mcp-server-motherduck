import { MotherDuck } from '@motherduck/sdk';

export default async function handler(req, res) {
  try {
    // Verify Bearer token
    const authHeader = req.headers['authorization'] || '';
    const token = authHeader.replace('Bearer ', '').trim();
    if (!token || token !== process.env.API_BEARER_TOKEN) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    // Validate env
    if (!process.env.MOTHERDUCK_API_KEY) {
      return res.status(500).json({ error: 'Missing MOTHERDUCK_API_KEY env var' });
    }

    // Parse request body
    const { action, sql, database } = req.body || {};
    if (!action || !sql) {
      return res.status(400).json({ error: 'Missing action or sql in body' });
    }

    // Connect to MotherDuck
    const client = new MotherDuck({
      apiKey: process.env.MOTHERDUCK_API_KEY
    });

    const db = await client.database(database || process.env.MOTHERDUCK_DATABASE || 'default');

    let result;
    if (action === 'query') {
      result = await db.query(sql);
    } else {
      return res.status(400).json({ error: `Unsupported action: ${action}` });
    }

    return res.status(200).json({ rows: result });
  } catch (err) {
    console.error('MotherDuck MCP Error:', err);
    return res.status(500).json({ error: 'Internal MCP Server Error', details: err.message });
  }
}
