import { readFileSync } from 'node:fs';
import { isDeepStrictEqual } from 'node:util';
import pg from 'pg';
import { eventKey, interpret, InvalidEvent, scenario, validate } from './domain.js';

const decoder = new TextDecoder('utf-8', { fatal: true });
const schemaSql = readFileSync(new URL('../db/schema.sql', import.meta.url), 'utf8');

export class History {
  constructor({ connectionString = process.env.DATABASE_URL, schema = process.env.DB_SCHEMA || 'public' } = {}) {
    if (!/^[a-z_][a-z0-9_]*$/.test(schema)) throw new Error('Invalid PostgreSQL schema name');
    this.schema = schema;
    this.pool = new pg.Pool({ connectionString, options: `-c search_path=${schema}`, connectionTimeoutMillis: 10000, max: 5 });
    this.pool.on('error', (error) => console.error(`PostgreSQL connection error: ${error.message}`));
  }

  async init() {
    await this.transaction(async (client) => {
      // Serialize DDL when a CLI and sink start together. Schema name is validated above.
      await client.query('SELECT pg_advisory_xact_lock(hashtext($1))', [`fiber-init-${this.schema}`]);
      await client.query(`CREATE SCHEMA IF NOT EXISTS "${this.schema}"`);
      await client.query(schemaSql);
    });
    return this;
  }

  async transaction(work, begin = 'BEGIN') {
    const client = await this.pool.connect();
    try {
      await client.query(begin);
      const result = await work(client);
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  close() { return this.pool.end(); }

  async recordHeartbeat(name, status) {
    await this.pool.query(`INSERT INTO service_heartbeats (name,status) VALUES ($1,$2)
      ON CONFLICT (name) DO UPDATE SET status=EXCLUDED.status, seen_at=clock_timestamp()`, [name, status]);
  }

  async receipt(client, { topic, partition, offset }, eventId, result) {
    await client.query('INSERT INTO receipts (topic, partition_id, kafka_offset, event_id, result) VALUES ($1,$2,$3,$4,$5)',
      [topic, partition, offset, eventId, result]);
  }

  async ingest(raw, key, location) {
    // Only content/identity failures are quarantined; database errors propagate.
    let event;
    let invalidReason;
    try {
      if (raw === null) throw new InvalidEvent('Tombstone records are not prior-authorization events');
      event = JSON.parse(decoder.decode(raw));
      validate(event);
      if (key === null || decoder.decode(key) !== eventKey(event)) throw new InvalidEvent('Kafka key must match tenant and source encounter');
    } catch (error) {
      if (!(error instanceof InvalidEvent || error instanceof SyntaxError || error.code === 'ERR_ENCODING_INVALID_ENCODED_DATA')) throw error;
      invalidReason = error.message;
    }
    return this.transaction(async (client) => {
      let result = 'rejected';
      if (!invalidReason) {
        const inserted = await client.query(`
          INSERT INTO events (event_id,tenant_id,source_account_id,source_encounter_id,source_sequence,topic,partition_id,kafka_offset,payload)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT DO NOTHING RETURNING event_id`,
        [event.event_id, event.tenant_id, event.source_account_id, event.source_encounter_id, event.source_sequence,
          location.topic, location.partition, location.offset, JSON.stringify(event)]);
        if (inserted.rowCount) {
          result = 'stored';
        } else {
          // ON CONFLICT also handles simultaneous deliveries safely across consumers.
          const existing = await client.query('SELECT payload FROM events WHERE event_id=$1', [event.event_id]);
          if (existing.rowCount && isDeepStrictEqual(existing.rows[0].payload, event)) result = 'duplicate';
          else invalidReason = existing.rowCount ? 'Conflicting payload for an existing event_id' : 'Conflicting event for an existing source sequence';
        }
      }
      if (invalidReason) {
        await client.query('INSERT INTO rejections (topic,partition_id,kafka_offset,reason,raw_base64) VALUES ($1,$2,$3,$4,$5)',
          [location.topic, location.partition, location.offset, invalidReason, (raw || Buffer.alloc(0)).toString('base64')]);
      }
      await this.receipt(client, location, result === 'rejected' ? null : event.event_id, result);
      return result;
    });
  }

  async events(client = this.pool) {
    return (await client.query('SELECT * FROM events ORDER BY ingest_id')).rows;
  }

  async receiptCursor() {
    return (await this.pool.query('SELECT COALESCE(MAX(id),0)::text AS cursor FROM receipts')).rows[0].cursor;
  }

  async receiptsAfter(cursor) {
    return (await this.pool.query('SELECT * FROM receipts WHERE id>$1 ORDER BY id', [cursor])).rows;
  }

  async inspect() {
    return this.transaction(async (client) => ({
      events: await this.events(client),
      receipts: (await client.query('SELECT * FROM receipts ORDER BY id')).rows,
      rejections: (await client.query('SELECT * FROM rejections ORDER BY id')).rows,
      flow_batches: (await client.query('SELECT * FROM flow_batches ORDER BY id')).rows,
    }), 'BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
  }

  async savedScenario() {
    await this.pool.query("INSERT INTO demo_scenarios (name,events) VALUES ('default',$1) ON CONFLICT DO NOTHING", [JSON.stringify(scenario())]);
    return (await this.pool.query("SELECT events FROM demo_scenarios WHERE name='default'")).rows[0].events;
  }

  async pull(crosswalk, claims) {
    return this.transaction(async (client) => {
      const rows = await this.events(client);
      const report = { ...interpret(rows.map((row) => row.payload), crosswalk, claims),
        pulled_at: new Date().toISOString(), history_event_count: rows.length,
        through_ingest_id: rows.at(-1)?.ingest_id || '0' };
      const batch = await client.query('INSERT INTO flow_batches (through_ingest_id,report) VALUES ($1,$2) RETURNING id',
        [report.through_ingest_id, JSON.stringify(report)]);
      return { ...report, batch_id: batch.rows[0].id };
    }, 'BEGIN ISOLATION LEVEL REPEATABLE READ');
  }
}

