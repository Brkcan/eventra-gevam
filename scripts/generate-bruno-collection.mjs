import fs from 'node:fs/promises';
import path from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

const execFileAsync = promisify(execFile);

const rootDir = process.cwd();
const outputDir = path.join(rootDir, 'bruno', 'Eventra-Local');
const zipPath = path.join(rootDir, 'bruno', 'Eventra-Local.zip');

const envVars = {
  apiBaseUrl: 'http://localhost:3001',
  cacheLoaderBaseUrl: 'http://localhost:3010',
  customerId: 'cust-123',
  journeyId: 'cart_abandonment_v1',
  journeyVersion: 1,
  fromVersion: 1,
  toVersion: 2,
  waitNodeId: 'wait-1',
  eventType: 'cart_add',
  segmentKey: 'vip',
  templateId: 'welcome_email_v1',
  endpointId: 'crm_webhook',
  instanceId: 'instance-123',
  connectionId: '11111111-1111-1111-1111-111111111111',
  jobId: '22222222-2222-2222-2222-222222222222'
};

const requests = [
  {
    folder: 'API/Core',
    name: 'Health',
    method: 'get',
    url: '{{apiBaseUrl}}/health'
  },
  {
    folder: 'API/Core',
    name: 'Ingest Event',
    method: 'post',
    url: '{{apiBaseUrl}}/ingest',
    body: {
      customer_id: '{{customerId}}',
      event_type: '{{eventType}}',
      payload: {
        product_id: 'p-42',
        price: 1499
      },
      source: 'web'
    }
  },
  {
    folder: 'API/Dashboard',
    name: 'KPI',
    method: 'get',
    url: '{{apiBaseUrl}}/dashboard/kpi'
  },
  {
    folder: 'API/Dashboard',
    name: 'Journey Performance',
    method: 'get',
    url: '{{apiBaseUrl}}/dashboard/journey-performance'
  },
  {
    folder: 'API/Dashboard',
    name: 'Cache Health',
    method: 'get',
    url: '{{apiBaseUrl}}/dashboard/cache-health'
  },
  {
    folder: 'API/Management',
    name: 'Get Global Pause',
    method: 'get',
    url: '{{apiBaseUrl}}/management/global-pause'
  },
  {
    folder: 'API/Management',
    name: 'Set Global Pause',
    method: 'put',
    url: '{{apiBaseUrl}}/management/global-pause',
    body: {
      enabled: false
    }
  },
  {
    folder: 'API/Management',
    name: 'List Release Controls',
    method: 'get',
    url: '{{apiBaseUrl}}/management/release-controls'
  },
  {
    folder: 'API/Management',
    name: 'Update Release Control',
    method: 'put',
    url: '{{apiBaseUrl}}/management/release-controls/{{journeyId}}',
    body: {
      version: '{{journeyVersion}}',
      rollout_percent: 100,
      release_paused: false
    }
  },
  {
    folder: 'API/Catalogues',
    name: 'Summary',
    method: 'get',
    url: '{{apiBaseUrl}}/catalogues/summary'
  },
  {
    folder: 'API/Catalogues',
    name: 'Cache Datasets',
    method: 'get',
    url: '{{apiBaseUrl}}/catalogues/cache-datasets'
  },
  {
    folder: 'API/Catalogues',
    name: 'List Event Types',
    method: 'get',
    url: '{{apiBaseUrl}}/catalogues/event-types?q=cart&limit=20&offset=0'
  },
  {
    folder: 'API/Catalogues',
    name: 'Create Event Type',
    method: 'post',
    url: '{{apiBaseUrl}}/catalogues/event-types',
    body: {
      event_type: '{{eventType}}',
      description: 'Cart add event',
      owner: 'growth-team',
      version: 1,
      required_fields: ['product_id', 'price'],
      schema_json: {
        type: 'object'
      },
      sample_payload: {
        product_id: 'p-42',
        price: 1499
      },
      is_active: true
    }
  },
  {
    folder: 'API/Catalogues',
    name: 'Update Event Type',
    method: 'put',
    url: '{{apiBaseUrl}}/catalogues/event-types/{{eventType}}',
    body: {
      description: 'Updated cart add event',
      owner: 'growth-team',
      version: 2,
      required_fields: ['product_id', 'price'],
      schema_json: {
        type: 'object'
      },
      sample_payload: {
        product_id: 'p-42',
        price: 1499
      },
      is_active: true
    }
  },
  {
    folder: 'API/Catalogues',
    name: 'Delete Event Type',
    method: 'delete',
    url: '{{apiBaseUrl}}/catalogues/event-types/{{eventType}}'
  },
  {
    folder: 'API/Catalogues',
    name: 'List Segments',
    method: 'get',
    url: '{{apiBaseUrl}}/catalogues/segments?q=vip&limit=20&offset=0'
  },
  {
    folder: 'API/Catalogues',
    name: 'Create Segment',
    method: 'post',
    url: '{{apiBaseUrl}}/catalogues/segments',
    body: {
      segment_key: '{{segmentKey}}',
      display_name: 'VIP Customers',
      rule_expression: "attributes.tier >= 3",
      description: 'VIP segment',
      is_active: true
    }
  },
  {
    folder: 'API/Catalogues',
    name: 'Update Segment',
    method: 'put',
    url: '{{apiBaseUrl}}/catalogues/segments/{{segmentKey}}',
    body: {
      display_name: 'VIP Customers',
      rule_expression: "attributes.tier >= 4",
      description: 'Updated VIP segment',
      is_active: true
    }
  },
  {
    folder: 'API/Catalogues',
    name: 'Delete Segment',
    method: 'delete',
    url: '{{apiBaseUrl}}/catalogues/segments/{{segmentKey}}'
  },
  {
    folder: 'API/Catalogues',
    name: 'List Templates',
    method: 'get',
    url: '{{apiBaseUrl}}/catalogues/templates?q=welcome&limit=20&offset=0'
  },
  {
    folder: 'API/Catalogues',
    name: 'Create Template',
    method: 'post',
    url: '{{apiBaseUrl}}/catalogues/templates',
    body: {
      template_id: '{{templateId}}',
      channel: 'email',
      subject: 'Welcome {{attributes.first_name}}',
      body: 'Hello {{attributes.first_name}}, welcome to Eventra.',
      variables: ['attributes.first_name'],
      is_active: true
    }
  },
  {
    folder: 'API/Catalogues',
    name: 'Update Template',
    method: 'put',
    url: '{{apiBaseUrl}}/catalogues/templates/{{templateId}}',
    body: {
      channel: 'email',
      subject: 'Welcome back {{attributes.first_name}}',
      body: 'Hello {{attributes.first_name}}, nice to see you again.',
      variables: ['attributes.first_name'],
      is_active: true
    }
  },
  {
    folder: 'API/Catalogues',
    name: 'Delete Template',
    method: 'delete',
    url: '{{apiBaseUrl}}/catalogues/templates/{{templateId}}'
  },
  {
    folder: 'API/Catalogues',
    name: 'List Endpoints',
    method: 'get',
    url: '{{apiBaseUrl}}/catalogues/endpoints?q=crm&limit=20&offset=0'
  },
  {
    folder: 'API/Catalogues',
    name: 'Create Endpoint',
    method: 'post',
    url: '{{apiBaseUrl}}/catalogues/endpoints',
    body: {
      endpoint_id: '{{endpointId}}',
      method: 'POST',
      url: 'https://example.com/webhook',
      headers: {
        authorization: 'Bearer token'
      },
      timeout_ms: 5000,
      description: 'CRM webhook',
      is_active: true
    }
  },
  {
    folder: 'API/Catalogues',
    name: 'Update Endpoint',
    method: 'put',
    url: '{{apiBaseUrl}}/catalogues/endpoints/{{endpointId}}',
    body: {
      method: 'POST',
      url: 'https://example.com/webhook',
      headers: {
        authorization: 'Bearer token'
      },
      timeout_ms: 8000,
      description: 'Updated CRM webhook',
      is_active: true
    }
  },
  {
    folder: 'API/Catalogues',
    name: 'Delete Endpoint',
    method: 'delete',
    url: '{{apiBaseUrl}}/catalogues/endpoints/{{endpointId}}'
  },
  {
    folder: 'API/Journeys',
    name: 'List Journeys',
    method: 'get',
    url: '{{apiBaseUrl}}/journeys?journey_id={{journeyId}}&status=draft&limit=20&offset=0&sort_by=journey_id&sort_order=asc'
  },
  {
    folder: 'API/Journeys',
    name: 'Create Or Update Journey',
    method: 'post',
    url: '{{apiBaseUrl}}/journeys',
    body: {
      journey_id: '{{journeyId}}',
      version: '{{journeyVersion}}',
      name: 'Cart Abandonment 30m',
      status: 'draft',
      folder_path: 'Workspace',
      graph_json: {
        nodes: [
          { id: 'trigger', type: 'trigger', data: { node_kind: 'trigger' } },
          { id: '{{waitNodeId}}', type: 'wait', data: { node_kind: 'wait', manual_release: true } }
        ],
        edges: [
          { id: 'e1', source: 'trigger', target: '{{waitNodeId}}' }
        ]
      }
    }
  },
  {
    folder: 'API/Journeys',
    name: 'Get Journey Approval',
    method: 'get',
    url: '{{apiBaseUrl}}/journeys/{{journeyId}}/approval?version={{journeyVersion}}'
  },
  {
    folder: 'API/Journeys',
    name: 'Request Journey Approval',
    method: 'post',
    url: '{{apiBaseUrl}}/journeys/{{journeyId}}/request-approval',
    body: {
      version: '{{journeyVersion}}',
      requested_by: 'burakcan',
      note: 'Ready for review'
    }
  },
  {
    folder: 'API/Journeys',
    name: 'Approve Journey',
    method: 'post',
    url: '{{apiBaseUrl}}/journeys/{{journeyId}}/approve',
    body: {
      version: '{{journeyVersion}}',
      reviewed_by: 'reviewer',
      note: 'Approved'
    }
  },
  {
    folder: 'API/Journeys',
    name: 'Reject Journey',
    method: 'post',
    url: '{{apiBaseUrl}}/journeys/{{journeyId}}/reject',
    body: {
      version: '{{journeyVersion}}',
      reviewed_by: 'reviewer',
      note: 'Needs changes'
    }
  },
  {
    folder: 'API/Journeys',
    name: 'Journey Diff',
    method: 'get',
    url: '{{apiBaseUrl}}/journeys/{{journeyId}}/diff?from_version={{fromVersion}}&to_version={{toVersion}}'
  },
  {
    folder: 'API/Journeys',
    name: 'List Journey Folders',
    method: 'get',
    url: '{{apiBaseUrl}}/journey-folders'
  },
  {
    folder: 'API/Journeys',
    name: 'Create Journey Folder',
    method: 'post',
    url: '{{apiBaseUrl}}/journey-folders',
    body: {
      folder_path: 'Workspace/Retention'
    }
  },
  {
    folder: 'API/Journeys',
    name: 'Delete Journey',
    method: 'delete',
    url: '{{apiBaseUrl}}/journeys/{{journeyId}}?version={{journeyVersion}}'
  },
  {
    folder: 'API/Journeys',
    name: 'Clone Journey Version',
    method: 'post',
    url: '{{apiBaseUrl}}/journeys/{{journeyId}}/clone-version',
    body: {
      source_version: '{{journeyVersion}}',
      target_version: '{{toVersion}}',
      name: 'Cart Abandonment 30m v2',
      status: 'draft'
    }
  },
  {
    folder: 'API/Journeys',
    name: 'Move Journey Folder',
    method: 'patch',
    url: '{{apiBaseUrl}}/journeys/{{journeyId}}/move-folder',
    body: {
      version: '{{journeyVersion}}',
      target_folder_path: 'Workspace/Retention'
    }
  },
  {
    folder: 'API/Customers',
    name: 'Create Customer',
    method: 'post',
    url: '{{apiBaseUrl}}/customers',
    body: {
      customer_id: '{{customerId}}',
      segment: '{{segmentKey}}',
      attributes: {
        first_name: 'Burak',
        email: 'burak@example.com',
        tier: 3
      }
    }
  },
  {
    folder: 'API/Customers',
    name: 'Upsert Customer Profile',
    method: 'put',
    url: '{{apiBaseUrl}}/customers/{{customerId}}/profile',
    body: {
      segment: '{{segmentKey}}',
      attributes: {
        first_name: 'Burak',
        email: 'burak@example.com',
        tier: 3
      }
    }
  },
  {
    folder: 'API/Customers',
    name: 'Get Customer Profile',
    method: 'get',
    url: '{{apiBaseUrl}}/customers/{{customerId}}/profile'
  },
  {
    folder: 'API/Customers',
    name: 'List Customers',
    method: 'get',
    url: '{{apiBaseUrl}}/customers?segment={{segmentKey}}&q={{customerId}}&limit=20&offset=0&sort_order=desc'
  },
  {
    folder: 'API/Operational',
    name: 'List Actions',
    method: 'get',
    url: '{{apiBaseUrl}}/actions?customer_id={{customerId}}&journey_id={{journeyId}}&channel=email&status=sent&limit=20&offset=0&sort_order=desc'
  },
  {
    folder: 'API/Operational',
    name: 'List External Calls',
    method: 'get',
    url: '{{apiBaseUrl}}/external-calls?customer_id={{customerId}}&journey_id={{journeyId}}&instance_id={{instanceId}}&result_type=success&limit=20&offset=0&sort_order=desc'
  },
  {
    folder: 'API/Operational',
    name: 'List Journey Instances',
    method: 'get',
    url: '{{apiBaseUrl}}/journey-instances?customer_id={{customerId}}&limit=20'
  },
  {
    folder: 'API/Operational',
    name: 'List Journey Instance Transitions',
    method: 'get',
    url: '{{apiBaseUrl}}/journey-instance-transitions?instance_id={{instanceId}}&customer_id={{customerId}}&journey_id={{journeyId}}&limit=20&offset=0'
  },
  {
    folder: 'API/Operational',
    name: 'Manual Wait Queue',
    method: 'get',
    url: '{{apiBaseUrl}}/manual-wait-queue?journey_id={{journeyId}}&version={{journeyVersion}}&wait_node_id={{waitNodeId}}&limit=20&offset=0'
  },
  {
    folder: 'API/Operational',
    name: 'Manual Wait Release',
    method: 'post',
    url: '{{apiBaseUrl}}/manual-wait-release',
    body: {
      journey_id: '{{journeyId}}',
      version: '{{journeyVersion}}',
      wait_node_id: '{{waitNodeId}}',
      count: 10,
      released_by: 'burakcan'
    }
  },
  {
    folder: 'API/Operational',
    name: 'DLQ Events',
    method: 'get',
    url: '{{apiBaseUrl}}/dlq-events?limit=20&offset=0'
  },
  {
    folder: 'API/Operational',
    name: 'Edge Capacity Usage',
    method: 'get',
    url: '{{apiBaseUrl}}/edge-capacity-usage?journey_id={{journeyId}}&edge_id=edge-1&window_type=daily&limit=20&offset=0'
  },
  {
    folder: 'Cache Loader',
    name: 'Health',
    method: 'get',
    url: '{{cacheLoaderBaseUrl}}/health'
  },
  {
    folder: 'Cache Loader',
    name: 'List Connections',
    method: 'get',
    url: '{{cacheLoaderBaseUrl}}/connections'
  },
  {
    folder: 'Cache Loader',
    name: 'Create Connection',
    method: 'post',
    url: '{{cacheLoaderBaseUrl}}/connections',
    body: {
      name: 'Source DB',
      host: 'host.docker.internal',
      port: 5432,
      database: 'analytics',
      username: 'analytics_user',
      password: 'secret',
      ssl: false
    }
  },
  {
    folder: 'Cache Loader',
    name: 'Delete Connection',
    method: 'delete',
    url: '{{cacheLoaderBaseUrl}}/connections/{{connectionId}}'
  },
  {
    folder: 'Cache Loader',
    name: 'Test Connection',
    method: 'post',
    url: '{{cacheLoaderBaseUrl}}/connections/{{connectionId}}/test'
  },
  {
    folder: 'Cache Loader',
    name: 'List Jobs',
    method: 'get',
    url: '{{cacheLoaderBaseUrl}}/jobs'
  },
  {
    folder: 'Cache Loader',
    name: 'Create Job',
    method: 'post',
    url: '{{cacheLoaderBaseUrl}}/jobs',
    body: {
      name: 'VIP Cache Refresh',
      connection_id: '{{connectionId}}',
      dataset_key: 'vip_customers',
      sql_query: 'select id, email, tier from customers',
      key_column: 'id',
      run_time: '07:00',
      timezone: 'Europe/Istanbul',
      enabled: true
    }
  },
  {
    folder: 'Cache Loader',
    name: 'Test Query',
    method: 'post',
    url: '{{cacheLoaderBaseUrl}}/jobs/test-query',
    body: {
      connection_id: '{{connectionId}}',
      sql_query: 'select id, email, tier from customers',
      key_column: 'id',
      preview_limit: 20
    }
  },
  {
    folder: 'Cache Loader',
    name: 'Update Job',
    method: 'put',
    url: '{{cacheLoaderBaseUrl}}/jobs/{{jobId}}',
    body: {
      name: 'VIP Cache Refresh',
      connection_id: '{{connectionId}}',
      dataset_key: 'vip_customers',
      sql_query: 'select id, email, tier from customers',
      key_column: 'id',
      run_time: '08:00',
      timezone: 'Europe/Istanbul',
      enabled: true
    }
  },
  {
    folder: 'Cache Loader',
    name: 'Delete Job',
    method: 'delete',
    url: '{{cacheLoaderBaseUrl}}/jobs/{{jobId}}'
  },
  {
    folder: 'Cache Loader',
    name: 'Run Job Now',
    method: 'post',
    url: '{{cacheLoaderBaseUrl}}/jobs/{{jobId}}/run-now'
  },
  {
    folder: 'Cache Loader',
    name: 'List Runs',
    method: 'get',
    url: '{{cacheLoaderBaseUrl}}/runs?job_id={{jobId}}'
  }
];

function sanitizeName(name) {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

function renderBru({ name, method, url, body, seq }) {
  const lines = [
    'meta {',
    `  name: ${name}`,
    '  type: http',
    `  seq: ${seq}`,
    '}',
    '',
    `${method} {`,
    `  url: ${url}`,
    `  body: ${body ? 'json' : 'none'}`,
    '  auth: none',
    '}',
    '',
    'headers {',
    '  content-type: application/json',
    '}'
  ];

  if (body) {
    lines.push('', 'body:json {');
    lines.push(JSON.stringify(body, null, 2));
    lines.push('}');
  }

  return `${lines.join('\n')}\n`;
}

function renderEnv(vars) {
  const lines = ['vars {'];
  for (const [key, value] of Object.entries(vars)) {
    lines.push(`  ${key}: ${String(value)}`);
  }
  lines.push('}');
  return `${lines.join('\n')}\n`;
}

async function ensureCleanDir(dir) {
  await fs.rm(dir, { recursive: true, force: true });
  await fs.mkdir(dir, { recursive: true });
}

async function writeCollection() {
  await ensureCleanDir(outputDir);
  await fs.mkdir(path.join(outputDir, 'environments'), { recursive: true });

  await fs.writeFile(
    path.join(outputDir, 'bruno.json'),
    JSON.stringify(
      {
        version: '1',
        name: 'Eventra Local',
        type: 'collection',
        ignore: ['node_modules', '.git']
      },
      null,
      2
    ) + '\n',
    'utf8'
  );

  await fs.writeFile(path.join(outputDir, 'environments', 'local.bru'), renderEnv(envVars), 'utf8');

  for (const [index, request] of requests.entries()) {
    const folderPath = path.join(outputDir, ...request.folder.split('/'));
    await fs.mkdir(folderPath, { recursive: true });
    const filePath = path.join(folderPath, `${sanitizeName(request.name)}.bru`);
    await fs.writeFile(filePath, renderBru({ ...request, seq: index + 1 }), 'utf8');
  }
}

async function makeZip() {
  await fs.rm(zipPath, { force: true });
  await execFileAsync('zip', ['-qr', zipPath, path.basename(outputDir)], {
    cwd: path.dirname(outputDir)
  });
}

await writeCollection();
await makeZip();

console.log(`Bruno collection created at: ${zipPath}`);
