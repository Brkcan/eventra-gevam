import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import ReactDOM from 'react-dom/client';
import {
  addEdge,
  Background,
  Controls,
  MiniMap,
  ReactFlow,
  useEdgesState,
  useNodesState
} from 'reactflow';
import 'reactflow/dist/style.css';
import './styles.css';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:3001';
const DEFAULT_FOLDER = 'Workspace';
const NAV_ITEMS = ['Scenarios', 'Catalogues', 'Management', 'Dashboards'];

function defaultLabelForKind(nodeKind) {
  if (nodeKind === 'trigger') return 'Trigger: cart_add';
  if (nodeKind === 'wait') return 'Wait: 30m';
  if (nodeKind === 'cache_lookup') return 'Cache Lookup';
  if (nodeKind === 'http_call') return 'HTTP Call';
  if (nodeKind === 'condition') return 'Condition: purchase yok';
  if (nodeKind === 'action') return 'Action: Email gonder';
  return 'Node';
}

function flowTypeForKind(nodeKind) {
  if (nodeKind === 'trigger') return 'input';
  if (nodeKind === 'action') return 'output';
  return undefined;
}

function normalizeNodeData(data = {}, fallbackId = '') {
  const nodeKind = data.node_kind || (fallbackId.includes('trigger') ? 'trigger' : 'condition');
  return {
    label: data.label || defaultLabelForKind(nodeKind),
    node_kind: nodeKind,
    event_type: data.event_type || 'cart_add',
    wait_minutes: Number(data.wait_minutes || 30),
    manual_release: Boolean(data.manual_release),
    cache_dataset_key: data.cache_dataset_key || '',
    cache_lookup_key_template: data.cache_lookup_key_template || '{{customer_id}}',
    cache_target_path: data.cache_target_path || 'external.cache.item',
    cache_on_miss: data.cache_on_miss || 'continue',
    cache_default_json: data.cache_default_json || '{}',
    http_method: data.http_method || 'POST',
    endpoint_id: data.endpoint_id || '',
    http_url: data.http_url || '',
    http_headers_json: data.http_headers_json || '{}',
    http_body_template: data.http_body_template || '{"customer_id":"{{customer_id}}"}',
    http_timeout_ms: Number(data.http_timeout_ms || 5000),
    response_mapping_json:
      data.response_mapping_json ||
      '{"score":"response.score","risk.level":"response.risk.level","decision":"response.decision"}',
    condition_key: data.condition_key || 'purchase_exists',
    condition_event_type: data.condition_event_type || 'purchase',
    condition_segment_value: data.condition_segment_value || 'vip',
    channel: data.channel || 'email',
    template_id: data.template_id || ''
  };
}

function fallbackPosition(index) {
  return { x: 120 + index * 260, y: 140 };
}

function nextNodeId(nodeKind) {
  return `${nodeKind}_${Date.now()}_${Math.floor(Math.random() * 1000)}`;
}

function parseJsonText(raw) {
  try {
    return { ok: true, value: JSON.parse(String(raw || '{}')) };
  } catch (error) {
    return { ok: false, message: error.message };
  }
}

function formatLogDate(iso) {
  if (!iso) {
    return '-';
  }
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) {
    return String(iso);
  }
  return date.toLocaleString('tr-TR');
}

function formatDurationMinutes(seconds) {
  const n = Number(seconds || 0);
  if (!Number.isFinite(n) || n <= 0) {
    return '0 dk';
  }
  return `${(n / 60).toFixed(1)} dk`;
}

function parseCsvList(raw) {
  return String(raw || '')
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
}

function toExpressionLiteral(rawValue) {
  const trimmed = String(rawValue ?? '').trim();
  if (!trimmed) {
    return '""';
  }
  if (trimmed === 'true' || trimmed === 'false') {
    return trimmed;
  }
  if (!Number.isNaN(Number(trimmed))) {
    return String(Number(trimmed));
  }
  return `"${trimmed.replace(/"/g, '\\"')}"`;
}

function createNode(nodeKind, position, id) {
  return {
    id,
    position,
    type: flowTypeForKind(nodeKind),
    data: normalizeNodeData({ node_kind: nodeKind }, id)
  };
}

function normalizeEdgeMeta(edge = {}) {
  const currentData = edge.data || {};
  const conditionResult = currentData.condition_result || '';
  const inferredType =
    currentData.edge_type ||
    (conditionResult === 'true' || conditionResult === 'false' ? conditionResult : 'always');
  const priority = Number.isFinite(Number(currentData.priority))
    ? Number(currentData.priority)
    : 100;
  const delayMinutes = Number.isFinite(Number(currentData.delay_minutes))
    ? Number(currentData.delay_minutes)
    : 0;
  const expression = String(currentData.expression || '').trim();
  const rateLimitPerDay = Number.isFinite(Number(currentData.rate_limit_per_day))
    ? Number(currentData.rate_limit_per_day)
    : 0;
  const maxCustomersTotal = Number.isFinite(Number(currentData.max_customers_total))
    ? Number(currentData.max_customers_total)
    : 0;
  const maxCustomersPerDay = Number.isFinite(Number(currentData.max_customers_per_day))
    ? Number(currentData.max_customers_per_day)
    : 0;

  return {
    ...edge,
    label:
      inferredType === 'always' &&
      delayMinutes === 0 &&
      priority === 100 &&
      !expression &&
      rateLimitPerDay === 0 &&
      maxCustomersTotal === 0 &&
      maxCustomersPerDay === 0
        ? undefined
        : `${inferredType} | p:${priority} | d:${delayMinutes}m | rl:${rateLimitPerDay}/d | cap:${maxCustomersTotal} | capd:${maxCustomersPerDay}`,
    data: {
      ...currentData,
      condition_result: inferredType === 'true' || inferredType === 'false' ? inferredType : '',
      edge_type: inferredType,
      priority,
      delay_minutes: delayMinutes,
      expression,
      rate_limit_per_day: rateLimitPerDay,
      max_customers_total: maxCustomersTotal,
      max_customers_per_day: maxCustomersPerDay
    }
  };
}

const defaultNodes = [
  {
    id: 'trigger',
    position: { x: 80, y: 120 },
    data: normalizeNodeData({ node_kind: 'trigger' }, 'trigger'),
    type: 'input'
  },
  {
    id: 'wait',
    position: { x: 340, y: 120 },
    data: normalizeNodeData({ node_kind: 'wait' }, 'wait')
  },
  {
    id: 'http_call',
    position: { x: 860, y: 120 },
    data: normalizeNodeData({ node_kind: 'http_call' }, 'http_call')
  },
  {
    id: 'condition',
    position: { x: 1120, y: 120 },
    data: normalizeNodeData({ node_kind: 'condition' }, 'condition')
  },
  {
    id: 'action',
    position: { x: 1380, y: 120 },
    data: normalizeNodeData({ node_kind: 'action' }, 'action'),
    type: 'output'
  },
  {
    id: 'cache_lookup',
    position: { x: 600, y: 120 },
    data: normalizeNodeData({ node_kind: 'cache_lookup' }, 'cache_lookup')
  }
];

const defaultEdges = [
  { id: 'e1', source: 'trigger', target: 'wait', animated: true },
  { id: 'e2', source: 'wait', target: 'cache_lookup' },
  { id: 'e3', source: 'cache_lookup', target: 'http_call' },
  { id: 'e4', source: 'http_call', target: 'condition' },
  { id: 'e5', source: 'condition', target: 'action' }
];

function getLatestJourney(items, journeyId) {
  const filtered = items.filter((item) => item.journey_id === journeyId);
  if (filtered.length === 0) {
    return null;
  }

  return filtered.sort((a, b) => b.version - a.version)[0];
}

function App() {
  const [nodes, setNodes, onNodesChange] = useNodesState(defaultNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(defaultEdges);
  const [journeyId, setJourneyId] = useState('cart_abandonment_v1');
  const [version, setVersion] = useState(1);
  const [name, setName] = useState('Cart Abandonment 30m');
  const [journeyStatus, setJourneyStatus] = useState('published');
  const [statusText, setStatusText] = useState('Hazir');
  const [busy, setBusy] = useState(false);
  const [selectedNodeId, setSelectedNodeId] = useState(null);
  const [selectedEdgeId, setSelectedEdgeId] = useState(null);
  const [journeyItems, setJourneyItems] = useState([]);
  const [folderItems, setFolderItems] = useState([DEFAULT_FOLDER]);
  const [selectedFolder, setSelectedFolder] = useState(DEFAULT_FOLDER);
  const [newFolderName, setNewFolderName] = useState('');
  const [activeMenu, setActiveMenu] = useState('Scenarios');
  const [viewMode, setViewMode] = useState('library');
  const [showJourneyMeta, setShowJourneyMeta] = useState(false);
  const [showInspector, setShowInspector] = useState(false);
  const [showJourneyLogs, setShowJourneyLogs] = useState(false);
  const [showManualQueue, setShowManualQueue] = useState(false);
  const [journeyLogs, setJourneyLogs] = useState([]);
  const [journeyLogsLoading, setJourneyLogsLoading] = useState(false);
  const [journeyLogsWindowLabel, setJourneyLogsWindowLabel] = useState('Henuz yuklenmedi');
  const [manualQueueItems, setManualQueueItems] = useState([]);
  const [manualQueueLoading, setManualQueueLoading] = useState(false);
  const [manualReleaseCount, setManualReleaseCount] = useState(10);
  const [manualWaitNodeId, setManualWaitNodeId] = useState('');
  const [dashboardKpi, setDashboardKpi] = useState(null);
  const [dashboardLoading, setDashboardLoading] = useState(false);
  const [dashboardJourneyPerf, setDashboardJourneyPerf] = useState({
    journeys: [],
    top_by_events: [],
    top_by_failures: []
  });
  const [dashboardCacheHealth, setDashboardCacheHealth] = useState({
    summary: {
      datasets_total: 0,
      datasets_loaded_today: 0,
      success_runs_today: 0,
      total_rows_last_success: 0
    },
    items: []
  });
  const [dashboardLastUpdated, setDashboardLastUpdated] = useState('');
  const [globalPauseEnabled, setGlobalPauseEnabled] = useState(false);
  const [releaseControls, setReleaseControls] = useState([]);
  const [managementLoading, setManagementLoading] = useState(false);
  const [cataloguesLoading, setCataloguesLoading] = useState(false);
  const [catalogueSummary, setCatalogueSummary] = useState(null);
  const [catalogueEventTypes, setCatalogueEventTypes] = useState([]);
  const [catalogueSegments, setCatalogueSegments] = useState([]);
  const [catalogueTemplates, setCatalogueTemplates] = useState([]);
  const [catalogueEndpoints, setCatalogueEndpoints] = useState([]);
  const [catalogueCacheDatasets, setCatalogueCacheDatasets] = useState([]);
  const [eventTypeForm, setEventTypeForm] = useState({
    event_type: '',
    description: '',
    owner: '',
    version: 1,
    required_fields_csv: '',
    schema_json: '{}',
    sample_payload: '{}',
    is_active: true
  });
  const [segmentForm, setSegmentForm] = useState({
    segment_key: '',
    display_name: '',
    rule_expression: '',
    description: '',
    is_active: true
  });
  const [templateForm, setTemplateForm] = useState({
    template_id: '',
    channel: 'email',
    subject: '',
    body: '',
    variables_csv: '',
    is_active: true
  });
  const [endpointForm, setEndpointForm] = useState({
    endpoint_id: '',
    method: 'POST',
    url: '',
    headers: '{}',
    timeout_ms: 5000,
    description: '',
    is_active: true
  });
  const [selectedJourneyKey, setSelectedJourneyKey] = useState('');
  const [exprBuilderDatasetKey, setExprBuilderDatasetKey] = useState('');
  const [exprBuilderColumn, setExprBuilderColumn] = useState('');
  const [exprBuilderOperator, setExprBuilderOperator] = useState('>=');
  const [exprBuilderValueType, setExprBuilderValueType] = useState('payload');
  const [exprBuilderValueInput, setExprBuilderValueInput] = useState('amount');
  const [draggedJourneyKey, setDraggedJourneyKey] = useState('');
  const [activeDropFolder, setActiveDropFolder] = useState('');
  const draggedJourneyKeyRef = useRef('');

  const selectedNode = useMemo(
    () => nodes.find((node) => node.id === selectedNodeId) || null,
    [nodes, selectedNodeId]
  );
  const selectedEdge = useMemo(
    () => edges.find((edge) => edge.id === selectedEdgeId) || null,
    [edges, selectedEdgeId]
  );
  const selectedJourneyItem = useMemo(
    () =>
      journeyItems.find((item) => `${item.journey_id}::${item.version}` === selectedJourneyKey) || null,
    [journeyItems, selectedJourneyKey]
  );
  const waitNodes = useMemo(
    () =>
      nodes.filter((node) => {
        const kind = node?.data?.node_kind || node?.type;
        return kind === 'wait';
      }),
    [nodes]
  );
  const activeEventTypeOptions = useMemo(
    () =>
      catalogueEventTypes
        .filter((item) => Boolean(item?.is_active))
        .map((item) => String(item.event_type)),
    [catalogueEventTypes]
  );
  const activeTemplateOptions = useMemo(
    () => catalogueTemplates.filter((item) => Boolean(item?.is_active)),
    [catalogueTemplates]
  );
  const activeSegmentOptions = useMemo(
    () => catalogueSegments.filter((item) => Boolean(item?.is_active)),
    [catalogueSegments]
  );
  const activeEndpointOptions = useMemo(
    () => catalogueEndpoints.filter((item) => Boolean(item?.is_active)),
    [catalogueEndpoints]
  );
  const activeCacheDatasetOptions = useMemo(
    () => (catalogueCacheDatasets || []).map((item) => String(item.dataset_key)).filter(Boolean),
    [catalogueCacheDatasets]
  );
  const cacheDatasetMap = useMemo(
    () =>
      new Map(
        (catalogueCacheDatasets || [])
          .filter((item) => item?.dataset_key)
          .map((item) => [String(item.dataset_key), item])
      ),
    [catalogueCacheDatasets]
  );
  const cacheLookupNodes = useMemo(
    () => nodes.filter((node) => normalizeNodeData(node.data, node.id).node_kind === 'cache_lookup'),
    [nodes]
  );
  const selectedBuilderDatasetColumns = useMemo(() => {
    const item = cacheDatasetMap.get(String(exprBuilderDatasetKey || ''));
    const cols = Array.isArray(item?.columns) ? item.columns : [];
    return cols.map((col) => String(col)).filter(Boolean);
  }, [cacheDatasetMap, exprBuilderDatasetKey]);
  const selectedBuilderDatasetColumnTypes = useMemo(() => {
    const item = cacheDatasetMap.get(String(exprBuilderDatasetKey || ''));
    const raw = item?.column_types;
    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
      return {};
    }
    return raw;
  }, [cacheDatasetMap, exprBuilderDatasetKey]);
  const selectedBuilderColumnType = useMemo(() => {
    return String(selectedBuilderDatasetColumnTypes?.[exprBuilderColumn] || 'string');
  }, [exprBuilderColumn, selectedBuilderDatasetColumnTypes]);
  const availableExprOperators = useMemo(() => {
    const t = selectedBuilderColumnType;
    if (t === 'number' || t === 'integer') {
      return ['>=', '<=', '==', '!=', '>', '<'];
    }
    if (t === 'boolean') {
      return ['==', '!='];
    }
    if (t === 'date') {
      return ['>=', '<=', '==', '!=', '>', '<'];
    }
    return ['==', '!=', 'contains'];
  }, [selectedBuilderColumnType]);
  const builderCacheTargetPath = useMemo(() => {
    const configuredPath = cacheLookupNodes
      .map((node) => normalizeNodeData(node.data, node.id).cache_target_path)
      .find((value) => String(value || '').trim());
    return configuredPath || 'external.cache.item';
  }, [cacheLookupNodes]);
  const topEventMax = useMemo(() => {
    const values = (dashboardJourneyPerf?.top_by_events || []).map((item) =>
      Number(item.triggered_24h || 0)
    );
    return Math.max(1, ...values, 1);
  }, [dashboardJourneyPerf]);
  const topFailMax = useMemo(() => {
    const values = (dashboardJourneyPerf?.top_by_failures || []).map((item) =>
      Number(item.failed_actions_24h || 0)
    );
    return Math.max(1, ...values, 1);
  }, [dashboardJourneyPerf]);

  const journeysInSelectedFolder = useMemo(
    () =>
      journeyItems.filter(
        (item) => String(item.folder_path || DEFAULT_FOLDER) === String(selectedFolder || DEFAULT_FOLDER)
      ),
    [journeyItems, selectedFolder]
  );
  const allFolders = useMemo(
    () =>
      Array.from(
        new Set([
          DEFAULT_FOLDER,
          ...folderItems,
          ...journeyItems.map((item) => item.folder_path || DEFAULT_FOLDER)
        ])
      ),
    [folderItems, journeyItems]
  );
  const folderCounts = useMemo(() => {
    const counts = {};
    for (const item of journeyItems) {
      const folder = item.folder_path || DEFAULT_FOLDER;
      counts[folder] = (counts[folder] || 0) + 1;
    }
    return counts;
  }, [journeyItems]);

  const onConnect = useCallback(
    (connection) =>
      setEdges((current) =>
        addEdge(
          normalizeEdgeMeta({ ...connection, id: `e_${Date.now()}`, animated: true }),
          current
        )
      ),
    [setEdges]
  );

  const onNodeClick = useCallback((_event, node) => {
    setSelectedNodeId(node.id);
    setSelectedEdgeId(null);
    setShowInspector(true);
  }, []);

  const onEdgeClick = useCallback((_event, edge) => {
    setSelectedEdgeId(edge.id);
    setSelectedNodeId(null);
    setShowInspector(true);
  }, []);

  const onPaneClick = useCallback(() => {
    setSelectedNodeId(null);
    setSelectedEdgeId(null);
    setShowInspector(false);
  }, []);

  const graphJson = useMemo(() => ({ nodes, edges }), [nodes, edges]);

  const addNode = useCallback(
    (nodeKind, forcedPosition = null) => {
      const id = nextNodeId(nodeKind);
      const selected = nodes.find((node) => node.id === selectedNodeId) || null;
      const position =
        forcedPosition ||
        (selected
          ? { x: selected.position.x + 240, y: selected.position.y }
          : fallbackPosition(nodes.length));
      const node = createNode(nodeKind, position, id);

      setNodes((current) => [...current, node]);
      setSelectedNodeId(id);
      setSelectedEdgeId(null);

      if (selected) {
        const edgeId = `e_${selected.id}_${id}`;
        setEdges((current) => [
          ...current,
          normalizeEdgeMeta({ id: edgeId, source: selected.id, target: id, animated: true })
        ]);
      }
    },
    [nodes, selectedNodeId, setEdges, setNodes]
  );

  const deleteSelected = useCallback(() => {
    if (selectedNodeId) {
      setNodes((current) => current.filter((node) => node.id !== selectedNodeId));
      setEdges((current) =>
        current.filter((edge) => edge.source !== selectedNodeId && edge.target !== selectedNodeId)
      );
      setSelectedNodeId(null);
      return;
    }

    if (selectedEdgeId) {
      setEdges((current) => current.filter((edge) => edge.id !== selectedEdgeId));
      setSelectedEdgeId(null);
    }
  }, [selectedEdgeId, selectedNodeId, setEdges, setNodes]);

  const updateSelectedNode = useCallback(
    (changes) => {
      if (!selectedNodeId) {
        return;
      }

      setNodes((current) =>
        current.map((node) => {
          if (node.id !== selectedNodeId) {
            return node;
          }

          const nextData = { ...normalizeNodeData(node.data, node.id), ...changes };
          const nextKind = nextData.node_kind;
          const nextType = flowTypeForKind(nextKind);

          return {
            ...node,
            type: nextType,
            data: nextData
          };
        })
      );
    },
    [selectedNodeId, setNodes]
  );

  const prettifySelectedNodeJsonField = useCallback(
    (fieldName) => {
      if (!selectedNodeId) {
        return;
      }

      const node = nodes.find((item) => item.id === selectedNodeId);
      if (!node) {
        return;
      }

      const data = normalizeNodeData(node.data, node.id);
      const currentValue = String(data[fieldName] || '{}');
      const parsed = parseJsonText(currentValue);
      if (!parsed.ok) {
        setStatusText(`${fieldName} JSON hatasi: ${parsed.message}`);
        return;
      }

      const formatted = JSON.stringify(parsed.value, null, 2);
      updateSelectedNode({ [fieldName]: formatted });
      setStatusText(`${fieldName} formatlandi.`);
    },
    [nodes, selectedNodeId, updateSelectedNode]
  );

  const updateSelectedEdge = useCallback(
    (changes) => {
      if (!selectedEdgeId) {
        return;
      }

      setEdges((current) =>
        current.map((edge) => {
          if (edge.id !== selectedEdgeId) {
            return edge;
          }
          return normalizeEdgeMeta({
            ...edge,
            data: {
              ...(edge.data || {}),
              ...changes
            }
          });
        })
      );
    },
    [selectedEdgeId, setEdges]
  );

  const buildEdgeExpression = useCallback(() => {
    if (!selectedEdgeId) {
      return;
    }
    const datasetKey = String(exprBuilderDatasetKey || '').trim();
    if (!datasetKey) {
      setStatusText('Expression builder icin dataset secilmeli.');
      return;
    }
    const column = String(exprBuilderColumn || '').trim();
    if (!column) {
      setStatusText('Expression builder icin kolon secilmeli.');
      return;
    }

    const operator = String(exprBuilderOperator || '>=').trim();
    const left = `${builderCacheTargetPath}.${column}`;
    let right = '';
    if (exprBuilderValueType === 'static') {
      right = toExpressionLiteral(exprBuilderValueInput);
    } else {
      const sourcePath = String(exprBuilderValueInput || column).trim();
      if (!sourcePath) {
        setStatusText('payload/attributes degeri bos olamaz.');
        return;
      }
      right = `${exprBuilderValueType}.${sourcePath}`;
    }

    const expression = `${left} ${operator} ${right}`;
    updateSelectedEdge({ expression });
    setStatusText(`Expression olusturuldu: ${expression}`);
  }, [
    builderCacheTargetPath,
    exprBuilderColumn,
    exprBuilderDatasetKey,
    exprBuilderOperator,
    exprBuilderValueInput,
    exprBuilderValueType,
    selectedEdgeId,
    updateSelectedEdge
  ]);

  const applyJourneyToCanvas = useCallback(
    (journey) => {
      setJourneyId(journey.journey_id);
      setVersion(journey.version);
      setName(journey.name || journey.journey_id);
      setJourneyStatus(journey.status || 'published');
      setSelectedFolder(journey.folder_path || DEFAULT_FOLDER);
      setSelectedJourneyKey(`${journey.journey_id}::${journey.version}`);

      const loadedNodesRaw = Array.isArray(journey.graph_json?.nodes) ? journey.graph_json.nodes : [];
      const loadedEdgesRaw = Array.isArray(journey.graph_json?.edges) ? journey.graph_json.edges : [];
      const loadedEdges = loadedEdgesRaw.map((edge) => normalizeEdgeMeta(edge));

      const loadedNodes = loadedNodesRaw.map((node, index) => {
        const sourceData = node.data || {
          node_kind: node.type,
          event_type: node.event_type,
          wait_minutes: node.duration_minutes,
          condition_key: node.check,
          channel: node.channel
        };
        const normalized = normalizeNodeData(sourceData, node.id);
        return {
          ...node,
          type: flowTypeForKind(normalized.node_kind),
          data: normalized,
          position: node.position || fallbackPosition(index)
        };
      });

      if (loadedNodes.length > 0) {
        setNodes(loadedNodes);
      }
      if (loadedEdges.length > 0) {
        setEdges(loadedEdges);
      }
      setSelectedNodeId(null);
      setSelectedEdgeId(null);
      setViewMode('designer');
      setStatusText(`Yuklendi: ${journey.journey_id} v${journey.version}`);
    },
    [setEdges, setNodes]
  );

  const fetchJourneys = useCallback(async () => {
    const params = new URLSearchParams({
      limit: '500',
      offset: '0',
      sort_by: 'updated_at',
      sort_order: 'desc'
    });
    const response = await fetch(`${API_BASE_URL}/journeys?${params.toString()}`);
    if (!response.ok) {
      throw new Error(`Journey list failed: ${response.status}`);
    }
    const body = await response.json();
    const items = body.items || [];
    setJourneyItems(items);
    return items;
  }, []);

  const fetchFolders = useCallback(async () => {
    const response = await fetch(`${API_BASE_URL}/journey-folders`);
    if (!response.ok) {
      throw new Error(`Folder list failed: ${response.status}`);
    }
    const body = await response.json();
    const dbFolders = (body.items || []).map((item) => item.folder_path).filter(Boolean);
    const merged = Array.from(new Set([DEFAULT_FOLDER, ...dbFolders]));
    setFolderItems(merged);
    return merged;
  }, []);

  const fetchJourneyLogs = useCallback(async () => {
    if (!journeyId) {
      setJourneyLogs([]);
      return;
    }
    setJourneyLogsLoading(true);
    try {
      const params = new URLSearchParams({
        journey_id: journeyId,
        limit: '200',
        offset: '0'
      });
      const response = await fetch(
        `${API_BASE_URL}/journey-instance-transitions?${params.toString()}`
      );
      if (!response.ok) {
        throw new Error(`Journey logs failed: ${response.status}`);
      }
      const body = await response.json();
      const items = Array.isArray(body.items) ? body.items : [];
      const oneHourAgo = Date.now() - 60 * 60 * 1000;
      const filtered = items.filter((item) => {
        if (Number(item.journey_version) !== Number(version)) {
          return false;
        }
        const createdMs = new Date(item.created_at).getTime();
        if (Number.isNaN(createdMs)) {
          return false;
        }
        return createdMs >= oneHourAgo;
      });
      setJourneyLogs(filtered);
      setJourneyLogsWindowLabel('Son 1 saat');
    } catch (error) {
      setStatusText(`Journey log hatasi: ${error.message}`);
    } finally {
      setJourneyLogsLoading(false);
    }
  }, [journeyId, version]);

  const fetchManualQueue = useCallback(async () => {
    if (!journeyId || !manualWaitNodeId) {
      setManualQueueItems([]);
      return;
    }
    setManualQueueLoading(true);
    try {
      const params = new URLSearchParams({
        journey_id: journeyId,
        version: String(version),
        wait_node_id: manualWaitNodeId,
        limit: '500',
        offset: '0'
      });
      const response = await fetch(`${API_BASE_URL}/manual-wait-queue?${params.toString()}`);
      if (!response.ok) {
        const body = await response.json().catch(() => ({}));
        throw new Error(body.message || `Manual queue failed: ${response.status}`);
      }
      const body = await response.json();
      setManualQueueItems(Array.isArray(body.items) ? body.items : []);
      setStatusText(
        `Manual queue yuklendi: ${manualWaitNodeId} icin ${(body.items || []).length} kayit.`
      );
    } catch (error) {
      setStatusText(`Manual queue hatasi: ${error.message}`);
    } finally {
      setManualQueueLoading(false);
    }
  }, [journeyId, manualWaitNodeId, version]);

  const releaseManualQueue = useCallback(async () => {
    if (!journeyId || !manualWaitNodeId) {
      setStatusText('Release icin journey ve wait node secilmeli.');
      return;
    }
    try {
      setManualQueueLoading(true);
      const response = await fetch(`${API_BASE_URL}/manual-wait-release`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          journey_id: journeyId,
          version: Number(version),
          wait_node_id: manualWaitNodeId,
          count: Math.max(1, Number(manualReleaseCount) || 1),
          released_by: 'ui'
        })
      });
      const body = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(body.message || `Release failed: ${response.status}`);
      }
      setStatusText(
        `Manual release tamamlandi: ${body.released_count || 0} customer waiting state'e alindi.`
      );
      await fetchManualQueue();
    } catch (error) {
      setStatusText(`Manual release hatasi: ${error.message}`);
    } finally {
      setManualQueueLoading(false);
    }
  }, [fetchManualQueue, journeyId, manualReleaseCount, manualWaitNodeId, version]);

  const fetchDashboardKpi = useCallback(async () => {
    setDashboardLoading(true);
    try {
      const response = await fetch(`${API_BASE_URL}/dashboard/kpi`);
      if (!response.ok) {
        const body = await response.json().catch(() => ({}));
        throw new Error(body.message || `Dashboard KPI failed: ${response.status}`);
      }
      const body = await response.json();
      setDashboardKpi(body.item || null);
      setDashboardLastUpdated(new Date().toLocaleString('tr-TR'));
    } catch (error) {
      setStatusText(`Dashboard KPI hatasi: ${error.message}`);
    } finally {
      setDashboardLoading(false);
    }
  }, []);

  const fetchDashboardJourneyPerformance = useCallback(async () => {
    try {
      const response = await fetch(`${API_BASE_URL}/dashboard/journey-performance`);
      if (!response.ok) {
        const body = await response.json().catch(() => ({}));
        throw new Error(body.message || `Dashboard journey perf failed: ${response.status}`);
      }
      const body = await response.json();
      setDashboardJourneyPerf(
        body.item || {
          journeys: [],
          top_by_events: [],
          top_by_failures: []
        }
      );
      setDashboardLastUpdated(new Date().toLocaleString('tr-TR'));
    } catch (error) {
      setStatusText(`Journey performans hatasi: ${error.message}`);
    }
  }, []);

  const fetchDashboardCacheHealth = useCallback(async () => {
    try {
      const response = await fetch(`${API_BASE_URL}/dashboard/cache-health`);
      if (!response.ok) {
        const body = await response.json().catch(() => ({}));
        throw new Error(body.message || `Dashboard cache health failed: ${response.status}`);
      }
      const body = await response.json();
      setDashboardCacheHealth(
        body.item || {
          summary: {
            datasets_total: 0,
            datasets_loaded_today: 0,
            success_runs_today: 0,
            total_rows_last_success: 0
          },
          items: []
        }
      );
      setDashboardLastUpdated(new Date().toLocaleString('tr-TR'));
    } catch (error) {
      setStatusText(`Cache health hatasi: ${error.message}`);
    }
  }, []);

  const fetchManagementData = useCallback(async () => {
    setManagementLoading(true);
    try {
      const [pauseResponse, controlsResponse] = await Promise.all([
        fetch(`${API_BASE_URL}/management/global-pause`),
        fetch(`${API_BASE_URL}/management/release-controls`)
      ]);

      if (!pauseResponse.ok) {
        throw new Error(`Global pause fetch failed: ${pauseResponse.status}`);
      }
      if (!controlsResponse.ok) {
        throw new Error(`Release controls fetch failed: ${controlsResponse.status}`);
      }

      const pauseBody = await pauseResponse.json();
      const controlsBody = await controlsResponse.json();
      setGlobalPauseEnabled(Boolean(pauseBody?.item?.enabled));
      setReleaseControls(Array.isArray(controlsBody.items) ? controlsBody.items : []);
    } catch (error) {
      setStatusText(`Management fetch hatasi: ${error.message}`);
    } finally {
      setManagementLoading(false);
    }
  }, []);

  const saveGlobalPause = useCallback(async () => {
    try {
      setManagementLoading(true);
      const response = await fetch(`${API_BASE_URL}/management/global-pause`, {
        method: 'PUT',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ enabled: Boolean(globalPauseEnabled) })
      });
      const body = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(body.message || `Global pause save failed: ${response.status}`);
      }
      setStatusText(`Global pause ${globalPauseEnabled ? 'aktif' : 'pasif'} yapildi.`);
    } catch (error) {
      setStatusText(`Global pause hatasi: ${error.message}`);
    } finally {
      setManagementLoading(false);
    }
  }, [globalPauseEnabled]);

  const saveReleaseControl = useCallback(
    async (journeyIdToSave) => {
      const item = releaseControls.find((row) => row.journey_id === journeyIdToSave);
      if (!item) {
        return;
      }
      try {
        setManagementLoading(true);
        const response = await fetch(
          `${API_BASE_URL}/management/release-controls/${encodeURIComponent(journeyIdToSave)}`,
          {
            method: 'PUT',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({
              version: Number(item.journey_version),
              rollout_percent: Number(item.rollout_percent),
              release_paused: Boolean(item.release_paused)
            })
          }
        );
        const body = await response.json().catch(() => ({}));
        if (!response.ok) {
          throw new Error(body.message || `Release control save failed: ${response.status}`);
        }
        setStatusText(
          `${journeyIdToSave} release control kaydedildi (rollout:${item.rollout_percent}%, paused:${item.release_paused}).`
        );
        await fetchManagementData();
      } catch (error) {
        setStatusText(`Release control hatasi: ${error.message}`);
      } finally {
        setManagementLoading(false);
      }
    },
    [fetchManagementData, releaseControls]
  );

  const fetchCatalogues = useCallback(async () => {
    setCataloguesLoading(true);
    try {
      const [summaryRes, eventTypesRes, segmentsRes, templatesRes, endpointsRes, cacheDatasetsRes] = await Promise.all([
        fetch(`${API_BASE_URL}/catalogues/summary`),
        fetch(`${API_BASE_URL}/catalogues/event-types?limit=200&offset=0`),
        fetch(`${API_BASE_URL}/catalogues/segments?limit=200&offset=0`),
        fetch(`${API_BASE_URL}/catalogues/templates?limit=200&offset=0`),
        fetch(`${API_BASE_URL}/catalogues/endpoints?limit=200&offset=0`),
        fetch(`${API_BASE_URL}/catalogues/cache-datasets`)
      ]);

      if (!summaryRes.ok) throw new Error(`Catalogue summary failed: ${summaryRes.status}`);
      if (!eventTypesRes.ok) throw new Error(`Event types failed: ${eventTypesRes.status}`);
      if (!segmentsRes.ok) throw new Error(`Segments failed: ${segmentsRes.status}`);
      if (!templatesRes.ok) throw new Error(`Templates failed: ${templatesRes.status}`);
      if (!endpointsRes.ok) throw new Error(`Endpoints failed: ${endpointsRes.status}`);
      if (!cacheDatasetsRes.ok) throw new Error(`Cache datasets failed: ${cacheDatasetsRes.status}`);

      const [summaryBody, eventTypesBody, segmentsBody, templatesBody, endpointsBody, cacheDatasetsBody] = await Promise.all([
        summaryRes.json(),
        eventTypesRes.json(),
        segmentsRes.json(),
        templatesRes.json(),
        endpointsRes.json(),
        cacheDatasetsRes.json()
      ]);

      setCatalogueSummary(summaryBody.item || null);
      setCatalogueEventTypes(Array.isArray(eventTypesBody.items) ? eventTypesBody.items : []);
      setCatalogueSegments(Array.isArray(segmentsBody.items) ? segmentsBody.items : []);
      setCatalogueTemplates(Array.isArray(templatesBody.items) ? templatesBody.items : []);
      setCatalogueEndpoints(Array.isArray(endpointsBody.items) ? endpointsBody.items : []);
      setCatalogueCacheDatasets(Array.isArray(cacheDatasetsBody.items) ? cacheDatasetsBody.items : []);
    } catch (error) {
      setStatusText(`Catalogues fetch hatasi: ${error.message}`);
    } finally {
      setCataloguesLoading(false);
    }
  }, []);

  const saveEventType = useCallback(async () => {
    const eventType = String(eventTypeForm.event_type || '').trim();
    if (!eventType) {
      setStatusText('event_type zorunlu.');
      return;
    }
    const parsed = parseJsonText(eventTypeForm.sample_payload || '{}');
    if (!parsed.ok || typeof parsed.value !== 'object' || Array.isArray(parsed.value)) {
      setStatusText(`sample_payload JSON hatasi: ${parsed.message}`);
      return;
    }
    const schemaParsed = parseJsonText(eventTypeForm.schema_json || '{}');
    if (!schemaParsed.ok || typeof schemaParsed.value !== 'object' || Array.isArray(schemaParsed.value)) {
      setStatusText(`schema_json JSON hatasi: ${schemaParsed.message}`);
      return;
    }
    try {
      setCataloguesLoading(true);
      const response = await fetch(`${API_BASE_URL}/catalogues/event-types`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          event_type: eventType,
          description: eventTypeForm.description || '',
          owner: eventTypeForm.owner || '',
          version: Math.max(1, Number(eventTypeForm.version) || 1),
          required_fields: parseCsvList(eventTypeForm.required_fields_csv),
          schema_json: schemaParsed.value,
          sample_payload: parsed.value,
          is_active: Boolean(eventTypeForm.is_active)
        })
      });
      const body = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(body.message || `Event type save failed: ${response.status}`);
      setStatusText(`Event type kaydedildi: ${eventType}`);
      await fetchCatalogues();
    } catch (error) {
      setStatusText(`Event type kayit hatasi: ${error.message}`);
    } finally {
      setCataloguesLoading(false);
    }
  }, [eventTypeForm, fetchCatalogues]);

  const saveSegment = useCallback(async () => {
    const segmentKey = String(segmentForm.segment_key || '').trim();
    if (!segmentKey) {
      setStatusText('segment_key zorunlu.');
      return;
    }
    try {
      setCataloguesLoading(true);
      const response = await fetch(`${API_BASE_URL}/catalogues/segments`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          segment_key: segmentKey,
          display_name: segmentForm.display_name || '',
          rule_expression: segmentForm.rule_expression || '',
          description: segmentForm.description || '',
          is_active: Boolean(segmentForm.is_active)
        })
      });
      const body = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(body.message || `Segment save failed: ${response.status}`);
      setStatusText(`Segment kaydedildi: ${segmentKey}`);
      await fetchCatalogues();
    } catch (error) {
      setStatusText(`Segment kayit hatasi: ${error.message}`);
    } finally {
      setCataloguesLoading(false);
    }
  }, [fetchCatalogues, segmentForm]);

  const saveTemplate = useCallback(async () => {
    const templateId = String(templateForm.template_id || '').trim();
    if (!templateId) {
      setStatusText('template_id zorunlu.');
      return;
    }
    try {
      setCataloguesLoading(true);
      const response = await fetch(`${API_BASE_URL}/catalogues/templates`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          template_id: templateId,
          channel: templateForm.channel,
          subject: templateForm.subject || '',
          body: templateForm.body || '',
          variables: parseCsvList(templateForm.variables_csv),
          is_active: Boolean(templateForm.is_active)
        })
      });
      const body = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(body.message || `Template save failed: ${response.status}`);
      setStatusText(`Template kaydedildi: ${templateId}`);
      await fetchCatalogues();
    } catch (error) {
      setStatusText(`Template kayit hatasi: ${error.message}`);
    } finally {
      setCataloguesLoading(false);
    }
  }, [fetchCatalogues, templateForm]);

  const saveEndpoint = useCallback(async () => {
    const endpointId = String(endpointForm.endpoint_id || '').trim();
    if (!endpointId) {
      setStatusText('endpoint_id zorunlu.');
      return;
    }
    const headersParsed = parseJsonText(endpointForm.headers || '{}');
    if (!headersParsed.ok || typeof headersParsed.value !== 'object' || Array.isArray(headersParsed.value)) {
      setStatusText(`headers JSON hatasi: ${headersParsed.message}`);
      return;
    }
    try {
      setCataloguesLoading(true);
      const response = await fetch(`${API_BASE_URL}/catalogues/endpoints`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          endpoint_id: endpointId,
          method: endpointForm.method,
          url: endpointForm.url,
          headers: headersParsed.value,
          timeout_ms: Math.max(100, Number(endpointForm.timeout_ms) || 5000),
          description: endpointForm.description || '',
          is_active: Boolean(endpointForm.is_active)
        })
      });
      const body = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(body.message || `Endpoint save failed: ${response.status}`);
      setStatusText(`Endpoint kaydedildi: ${endpointId}`);
      await fetchCatalogues();
    } catch (error) {
      setStatusText(`Endpoint kayit hatasi: ${error.message}`);
    } finally {
      setCataloguesLoading(false);
    }
  }, [endpointForm, fetchCatalogues]);

  const deleteCatalogueItem = useCallback(
    async (type, key) => {
      const map = {
        event_type: `/catalogues/event-types/${encodeURIComponent(key)}`,
        segment: `/catalogues/segments/${encodeURIComponent(key)}`,
        template: `/catalogues/templates/${encodeURIComponent(key)}`,
        endpoint: `/catalogues/endpoints/${encodeURIComponent(key)}`
      };
      const endpoint = map[type];
      if (!endpoint) {
        return;
      }
      const confirmed = window.confirm(`${key} silinsin mi?`);
      if (!confirmed) {
        return;
      }
      try {
        setCataloguesLoading(true);
        const response = await fetch(`${API_BASE_URL}${endpoint}`, { method: 'DELETE' });
        const body = await response.json().catch(() => ({}));
        if (!response.ok) throw new Error(body.message || `Delete failed: ${response.status}`);
        setStatusText(`Silindi: ${key}`);
        await fetchCatalogues();
      } catch (error) {
        setStatusText(`Silme hatasi: ${error.message}`);
      } finally {
        setCataloguesLoading(false);
      }
    },
    [fetchCatalogues]
  );

  const toggleJourneyLogs = useCallback(async () => {
    if (showJourneyLogs) {
      setShowJourneyLogs(false);
      return;
    }
    setShowJourneyLogs(true);
    await fetchJourneyLogs();
  }, [fetchJourneyLogs, showJourneyLogs]);

  const saveJourney = useCallback(async (statusOverride = null) => {
    setBusy(true);
    setStatusText('Journey kaydediliyor...');

    try {
      const finalStatus = statusOverride || journeyStatus;
      for (const node of nodes) {
        const data = normalizeNodeData(node.data, node.id);
        if (data.node_kind === 'cache_lookup' && data.cache_on_miss === 'default') {
          const defaultParsed = parseJsonText(data.cache_default_json || '{}');
          if (!defaultParsed.ok) {
            throw new Error(`Invalid cache_default_json on ${node.id}`);
          }
        }
        if (data.node_kind !== 'http_call') {
          continue;
        }

        const headersParsed = parseJsonText(data.http_headers_json || '{}');
        if (!headersParsed.ok || typeof headersParsed.value !== 'object' || Array.isArray(headersParsed.value)) {
          throw new Error(`Invalid http_headers_json on ${node.id}`);
        }

        const mappingParsed = parseJsonText(data.response_mapping_json || '{}');
        if (!mappingParsed.ok || typeof mappingParsed.value !== 'object' || Array.isArray(mappingParsed.value)) {
          throw new Error(`Invalid response_mapping_json on ${node.id}`);
        }
      }

      const listParams = new URLSearchParams({
        journey_id: journeyId,
        limit: '500',
        offset: '0'
      });
      const listResponse = await fetch(`${API_BASE_URL}/journeys?${listParams.toString()}`);
      if (!listResponse.ok) {
        throw new Error(`Journey list fetch failed: ${listResponse.status}`);
      }
      const listBody = await listResponse.json();
      const exists = (listBody.items || []).some(
        (item) => item.journey_id === journeyId && Number(item.version) === Number(version)
      );

      if (exists) {
        const confirmed = window.confirm(
          `Journey ${journeyId} v${version} zaten var. Uzerine yazilsin mi?`
        );
        if (!confirmed) {
          setStatusText('Publish iptal edildi: mevcut version overwrite edilmedi.');
          setBusy(false);
          return;
        }
      }

      const payload = {
        journey_id: journeyId,
        version,
        name,
        status: finalStatus,
        folder_path: selectedFolder || DEFAULT_FOLDER,
        graph_json: graphJson
      };

      const response = await fetch(`${API_BASE_URL}/journeys`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(payload)
      });

      if (!response.ok) {
        const errBody = await response.json().catch(() => ({}));
        throw new Error(errBody.message || `Publish failed: ${response.status}`);
      }

      setJourneyStatus(finalStatus);
      setStatusText(`Kayit tamamlandi: ${journeyId} v${version} [${finalStatus}]`);
      await fetchJourneys();
      await fetchFolders();
      setSelectedJourneyKey(`${journeyId}::${version}`);
    } catch (error) {
      setStatusText(`Kayit hatasi: ${error.message}`);
    } finally {
      setBusy(false);
    }
  }, [fetchFolders, fetchJourneys, graphJson, journeyId, journeyStatus, name, nodes, selectedFolder, version]);

  const rollbackSelectedVersion = useCallback(async () => {
    if (!selectedJourneyItem) {
      setStatusText('Rollback icin listeden bir journey version sec.');
      return;
    }

    try {
      setBusy(true);
      setStatusText(`Rollback baslatildi: ${selectedJourneyItem.journey_id} v${selectedJourneyItem.version}`);
      const response = await fetch(
        `${API_BASE_URL}/journeys/${encodeURIComponent(selectedJourneyItem.journey_id)}/clone-version`,
        {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({
            source_version: Number(selectedJourneyItem.version),
            status: 'published',
            name: `${selectedJourneyItem.name || selectedJourneyItem.journey_id} (rollback from v${selectedJourneyItem.version})`
          })
        }
      );
      const body = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(body.message || `Rollback failed: ${response.status}`);
      }

      const items = await fetchJourneys();
      const latest = getLatestJourney(items, selectedJourneyItem.journey_id);
      if (latest) {
        applyJourneyToCanvas(latest);
      }
      setStatusText(
        `Rollback tamamlandi: ${selectedJourneyItem.journey_id} v${body?.item?.version || 'new'} published`
      );
    } catch (error) {
      setStatusText(`Rollback hatasi: ${error.message}`);
    } finally {
      setBusy(false);
    }
  }, [applyJourneyToCanvas, fetchJourneys, selectedJourneyItem]);

  const newJourney = useCallback(() => {
    const seed = Date.now();
    setJourneyId(`journey_${seed}`);
    setName(`New Journey ${new Date().toLocaleString()}`);
    setVersion(1);
    setJourneyStatus('draft');
    setNodes([]);
    setEdges([]);
    setSelectedNodeId(null);
    setSelectedEdgeId(null);
    setSelectedJourneyKey('');
    setViewMode('designer');
    setStatusText('Yeni journey olusturuldu. Canvas bos, node ekleyerek baslayabilirsin.');
  }, [setEdges, setNodes]);

  const createFolder = useCallback(async () => {
    const folderPath = String(newFolderName || '').trim();
    if (!folderPath) {
      setStatusText('Klasor adi bos olamaz.');
      return;
    }

    try {
      setBusy(true);
      const response = await fetch(`${API_BASE_URL}/journey-folders`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ folder_path: folderPath })
      });
      const body = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(body.message || `Folder create failed: ${response.status}`);
      }
      await fetchFolders();
      setSelectedFolder(folderPath);
      setNewFolderName('');
      setStatusText(`Klasor olusturuldu: ${folderPath}`);
    } catch (error) {
      setStatusText(`Klasor olusturma hatasi: ${error.message}`);
    } finally {
      setBusy(false);
    }
  }, [fetchFolders, newFolderName]);

  const moveJourneyToFolder = useCallback(
    async (journeyItem, targetFolderOverride = null) => {
      const targetFolder = String(
        targetFolderOverride || journeyItem.folder_path || selectedFolder || DEFAULT_FOLDER
      ).trim();

      if (!targetFolder) {
        setStatusText('Hedef klasor bos olamaz.');
        return false;
      }
      if (targetFolder === (journeyItem.folder_path || DEFAULT_FOLDER)) {
        setStatusText('Journey zaten bu klasorde.');
        return false;
      }

      try {
        setBusy(true);
        let response = await fetch(
          `${API_BASE_URL}/journeys/${encodeURIComponent(journeyItem.journey_id)}/move-folder`,
          {
            method: 'PATCH',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({
              version: Number(journeyItem.version),
              target_folder_path: targetFolder
            })
          }
        );
        let body = await response.json().catch(() => ({}));
        let usedLegacyFallback = false;

        // Fallback: older API may not have /move-folder yet.
        if (response.status === 404) {
          const latest = await fetchJourneys();
          const source = (latest || []).find(
            (journey) =>
              journey.journey_id === journeyItem.journey_id &&
              Number(journey.version) === Number(journeyItem.version)
          );
          if (source) {
            response = await fetch(`${API_BASE_URL}/journeys`, {
              method: 'POST',
              headers: { 'content-type': 'application/json' },
              body: JSON.stringify({
                journey_id: source.journey_id,
                version: Number(source.version),
                name: source.name || source.journey_id,
                status: source.status || 'draft',
                folder_path: targetFolder,
                graph_json: source.graph_json || { nodes: [], edges: [] }
              })
            });
            body = await response.json().catch(() => ({}));
            usedLegacyFallback = true;
          }
        }

        if (!response.ok) {
          throw new Error(body.message || `Move failed: ${response.status}`);
        }
        if (!usedLegacyFallback && Object.prototype.hasOwnProperty.call(body || {}, 'moved')) {
          const movedCount = Number(body?.moved?.count || 0);
          if (movedCount <= 0) {
            throw new Error('move endpoint updated 0 rows');
          }
        }

        // Optimistic local state update to avoid stale list issues.
        setJourneyItems((current) =>
          current.map((journey) =>
            journey.journey_id === journeyItem.journey_id &&
            Number(journey.version) === Number(journeyItem.version)
              ? { ...journey, folder_path: targetFolder }
              : journey
          )
        );

        const refreshedItems = await fetchJourneys();
        await fetchFolders();
        const refreshedMovedItem = (refreshedItems || []).find(
          (journey) =>
            journey.journey_id === journeyItem.journey_id &&
            Number(journey.version) === Number(journeyItem.version)
        );
        if (
          refreshedMovedItem &&
          String(refreshedMovedItem.folder_path || DEFAULT_FOLDER) !== targetFolder
        ) {
          setJourneyItems((current) =>
            current.map((journey) =>
              journey.journey_id === journeyItem.journey_id &&
              Number(journey.version) === Number(journeyItem.version)
                ? { ...journey, folder_path: targetFolder }
                : journey
            )
          );
          setStatusText(
            `${journeyItem.journey_id} tasindi, liste cache oldugu icin client tarafinda duzeltildi.`
          );
          return true;
        }
        setStatusText(
          `${journeyItem.journey_id} v${journeyItem.version} -> ${targetFolder} tasindi.`
        );
        return true;
      } catch (error) {
        setStatusText(`Tasima hatasi: ${error.message}`);
        return false;
      } finally {
        setBusy(false);
      }
    },
    [fetchFolders, fetchJourneys, selectedFolder]
  );

  const onJourneyDragStart = useCallback((event, journeyItem) => {
    const key = `${journeyItem.journey_id}::${journeyItem.version}`;
    event.dataTransfer.setData('application/eventra-journey-key', key);
    event.dataTransfer.setData('text/plain', key);
    event.dataTransfer.effectAllowed = 'move';
    draggedJourneyKeyRef.current = key;
    setDraggedJourneyKey(key);
  }, []);

  const onJourneyDragEnd = useCallback(() => {
    // Drop sonrasi cleanup onFolderDrop icinde yapiliyor.
    setActiveDropFolder('');
  }, []);

  const onFolderDragOver = useCallback((event, folder) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = 'move';
    setActiveDropFolder(folder);
  }, []);

  const onFolderDragEnter = useCallback((event, folder) => {
    event.preventDefault();
    setActiveDropFolder(folder);
  }, []);

  const onFolderDrop = useCallback(
    async (event, folder) => {
      event.preventDefault();
      event.stopPropagation();
      const key =
        event.dataTransfer.getData('application/eventra-journey-key') ||
        event.dataTransfer.getData('text/plain') ||
        draggedJourneyKeyRef.current ||
        draggedJourneyKey;
      setActiveDropFolder('');
      if (!key) {
        setStatusText('Surukle-birak verisi okunamadi, tekrar dene.');
        return;
      }
      setSelectedFolder(folder);
      const item = journeyItems.find((journey) => `${journey.journey_id}::${journey.version}` === key);
      if (!item) {
        setStatusText('Tasima icin journey bulunamadi.');
        return;
      }
      const moved = await moveJourneyToFolder(item, folder);
      if (!moved) {
        return;
      }
      draggedJourneyKeyRef.current = '';
      setDraggedJourneyKey('');
    },
    [draggedJourneyKey, journeyItems, moveJourneyToFolder]
  );

  const deleteJourney = useCallback(async () => {
    if (!journeyId || !version) {
      setStatusText('Silme icin journey_id ve version gerekli.');
      return;
    }

    const confirmed = window.confirm(
      `${journeyId} v${version} silinsin mi? Bu islem geri alinmaz.`
    );
    if (!confirmed) {
      return;
    }

    try {
      setBusy(true);
      const response = await fetch(
        `${API_BASE_URL}/journeys/${encodeURIComponent(journeyId)}?version=${version}`,
        { method: 'DELETE' }
      );
      const body = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(body.message || `Delete failed: ${response.status}`);
      }

      await fetchJourneys();
      await fetchFolders();
      setSelectedJourneyKey('');
      newJourney();
      setStatusText(`${journeyId} v${version} silindi.`);
    } catch (error) {
      setStatusText(`Journey silme hatasi: ${error.message}`);
    } finally {
      setBusy(false);
    }
  }, [fetchFolders, fetchJourneys, journeyId, newJourney, version]);

  useEffect(() => {
    (async () => {
      try {
        await fetchJourneys();
        await fetchFolders();
      } catch (error) {
        setStatusText(`Journey/Folders yuklenemedi: ${error.message}`);
      }
    })();
  }, [fetchFolders, fetchJourneys]);

  useEffect(() => {
    if (!allFolders.includes(selectedFolder)) {
      setSelectedFolder(DEFAULT_FOLDER);
    }
  }, [allFolders, selectedFolder]);

  useEffect(() => {
    if (activeMenu !== 'Dashboards') {
      return;
    }
    fetchDashboardKpi();
    fetchDashboardJourneyPerformance();
    fetchDashboardCacheHealth();
  }, [activeMenu, fetchDashboardCacheHealth, fetchDashboardJourneyPerformance, fetchDashboardKpi]);

  useEffect(() => {
    if (activeMenu !== 'Management') {
      return;
    }
    fetchManagementData();
  }, [activeMenu, fetchManagementData]);

  useEffect(() => {
    if (activeMenu !== 'Catalogues') {
      return;
    }
    fetchCatalogues();
  }, [activeMenu, fetchCatalogues]);

  useEffect(() => {
    if (activeMenu !== 'Scenarios') {
      return;
    }
    if (
      catalogueEventTypes.length > 0 ||
      catalogueSegments.length > 0 ||
      catalogueTemplates.length > 0 ||
      catalogueEndpoints.length > 0
    ) {
      return;
    }
    fetchCatalogues();
  }, [
    activeMenu,
    catalogueEndpoints.length,
    catalogueEventTypes.length,
    catalogueSegments.length,
    catalogueTemplates.length,
    fetchCatalogues
  ]);

  useEffect(() => {
    setJourneyLogs([]);
    setJourneyLogsWindowLabel('Henuz yuklenmedi');
    setManualQueueItems([]);
  }, [journeyId, version]);

  useEffect(() => {
    if (waitNodes.length === 0) {
      setManualWaitNodeId('');
      setManualQueueItems([]);
      return;
    }
    if (!manualWaitNodeId || !waitNodes.some((node) => node.id === manualWaitNodeId)) {
      setManualWaitNodeId(waitNodes[0].id);
    }
  }, [manualWaitNodeId, waitNodes]);

  useEffect(() => {
    if (!selectedEdgeId) {
      return;
    }
    const configuredDataset =
      cacheLookupNodes
        .map((node) => normalizeNodeData(node.data, node.id).cache_dataset_key)
        .find((key) => String(key || '').trim()) || '';
    const fallbackDataset = configuredDataset || activeCacheDatasetOptions[0] || '';
    if (
      fallbackDataset &&
      (!exprBuilderDatasetKey || !activeCacheDatasetOptions.includes(exprBuilderDatasetKey))
    ) {
      setExprBuilderDatasetKey(fallbackDataset);
    }
  }, [activeCacheDatasetOptions, cacheLookupNodes, exprBuilderDatasetKey, selectedEdgeId]);

  useEffect(() => {
    if (!selectedEdgeId) {
      return;
    }
    if (selectedBuilderDatasetColumns.length === 0) {
      return;
    }
    if (!exprBuilderColumn || !selectedBuilderDatasetColumns.includes(exprBuilderColumn)) {
      setExprBuilderColumn(selectedBuilderDatasetColumns[0]);
    }
  }, [exprBuilderColumn, selectedBuilderDatasetColumns, selectedEdgeId]);

  useEffect(() => {
    if (!selectedEdgeId) {
      return;
    }
    if (!availableExprOperators.includes(exprBuilderOperator)) {
      setExprBuilderOperator(availableExprOperators[0] || '==');
    }
  }, [availableExprOperators, exprBuilderOperator, selectedEdgeId]);

  useEffect(() => {
    if (!selectedEdgeId) {
      return;
    }
    if (exprBuilderValueType === 'static') {
      return;
    }
    const fallback = String(exprBuilderColumn || '').trim();
    if (fallback && !String(exprBuilderValueInput || '').trim()) {
      setExprBuilderValueInput(fallback);
    }
  }, [exprBuilderColumn, exprBuilderValueInput, exprBuilderValueType, selectedEdgeId]);

  const selectedData = selectedNode ? normalizeNodeData(selectedNode.data, selectedNode.id) : null;
  const headersValidation =
    selectedData?.node_kind === 'http_call'
      ? parseJsonText(selectedData.http_headers_json || '{}')
      : { ok: true };
  const mappingValidation =
    selectedData?.node_kind === 'http_call'
      ? parseJsonText(selectedData.response_mapping_json || '{}')
      : { ok: true };

  return (
    <div className="appShell">
      <aside className="leftMenu">
        <div className="brand">EVENTRA</div>
        <nav>
          {NAV_ITEMS.map((item) => (
            <button
              key={item}
              type="button"
              className={activeMenu === item ? 'navItem active' : 'navItem'}
              onClick={() => setActiveMenu(item)}
            >
              {item}
            </button>
          ))}
        </nav>
      </aside>

      <div className="page">
      {activeMenu === 'Scenarios' && viewMode === 'designer' && (
      <>
      <section className="designerTop">
        <div className="designerTitle">
          <strong>{name}</strong>
          <span>Workspace &gt; {selectedFolder} &gt; {journeyId} v{version}</span>
        </div>
        <div className="designerActions">
          <button type="button" onClick={() => setViewMode('library')} disabled={busy}>
            Library
          </button>
          <button
            type="button"
            onClick={() => setShowJourneyMeta((prev) => !prev)}
            disabled={busy}
          >
            {showJourneyMeta ? 'Hide Fields' : 'Journey Fields'}
          </button>
          <button
            type="button"
            onClick={() => setShowManualQueue((prev) => !prev)}
            disabled={busy}
          >
            {showManualQueue ? 'Hide Manual Queue' : 'Manual Queue'}
          </button>
          <button
            type="button"
            onClick={toggleJourneyLogs}
            disabled={busy || journeyLogsLoading}
          >
            {showJourneyLogs
              ? 'Hide Logs'
              : journeyLogsLoading
                ? 'Loading Logs...'
                : 'Load Last 1h Logs'}
          </button>
          <button type="button" onClick={newJourney} disabled={busy}>
            New
          </button>
          <button type="button" className="danger" onClick={deleteJourney} disabled={busy}>
            Delete Journey
          </button>
          <button type="button" onClick={() => saveJourney('draft')} disabled={busy}>
            Save Draft
          </button>
          <button
            type="button"
            className="primary"
            onClick={() => saveJourney('published')}
            disabled={busy}
          >
            Publish
          </button>
        </div>
      </section>

      {showJourneyMeta && (
      <section className="controlsBar">
          <div className="journeyMetaGrid">
            <label>
              Journey ID
              <input value={journeyId} onChange={(e) => setJourneyId(e.target.value)} />
            </label>
            <label>
              Version
              <input
                type="number"
                min="1"
                value={version}
                onChange={(e) => setVersion(Math.max(1, Number(e.target.value) || 1))}
              />
            </label>
            <label>
              Name
              <input value={name} onChange={(e) => setName(e.target.value)} />
            </label>
            <label>
              Status
              <select value={journeyStatus} onChange={(e) => setJourneyStatus(e.target.value)}>
                <option value="draft">draft</option>
                <option value="published">published</option>
                <option value="archived">archived</option>
              </select>
            </label>
            <label>
              Folder
              <select value={selectedFolder} onChange={(e) => setSelectedFolder(e.target.value)}>
                {allFolders.map((folder) => (
                  <option key={folder} value={folder}>
                    {folder}
                  </option>
                ))}
              </select>
            </label>
          </div>
      </section>
      )}

      <main className={showInspector ? 'workspace withInspector' : 'workspace noInspector'}>
        <section className="canvas">
          <div className="canvasTools">
            <button type="button" title="Add Trigger" aria-label="Add Trigger" onClick={() => addNode('trigger')}>
              <span>T</span> Trigger
            </button>
            <button type="button" title="Add Wait" aria-label="Add Wait" onClick={() => addNode('wait')}>
              <span>W</span> Wait
            </button>
            <button
              type="button"
              title="Add Cache Lookup"
              aria-label="Add Cache Lookup"
              onClick={() => addNode('cache_lookup')}
            >
              <span>K</span> Cache
            </button>
            <button type="button" title="Add HTTP Call" aria-label="Add HTTP Call" onClick={() => addNode('http_call')}>
              <span>H</span> HTTP
            </button>
            <button type="button" title="Add Condition" aria-label="Add Condition" onClick={() => addNode('condition')}>
              <span>C</span> Condition
            </button>
            <button type="button" title="Add Action" aria-label="Add Action" onClick={() => addNode('action')}>
              <span>A</span> Action
            </button>
            <button
              type="button"
              title="Delete Selected"
              aria-label="Delete Selected"
              onClick={deleteSelected}
              disabled={!selectedNodeId && !selectedEdgeId}
            >
              <span>X</span> Delete
            </button>
          </div>
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onConnect={onConnect}
            onNodeClick={onNodeClick}
            onEdgeClick={onEdgeClick}
            onPaneClick={onPaneClick}
            fitView
          >
            <MiniMap />
            <Controls />
            <Background gap={20} />
          </ReactFlow>
        </section>
        {showInspector && (
        <aside className="sidePanel">
          <h2>Node Ayarlari</h2>
          {!selectedNode && !selectedEdge && <p className="panelHint">Akista bir node veya edge sec.</p>}

          {selectedNode && selectedData && (
            <div className="panelFields">
              <label>
                Node ID
                <input value={selectedNode.id} readOnly />
              </label>

              <label>
                Node Type
                <select
                  value={selectedData.node_kind}
                  onChange={(e) => {
                    const nextKind = e.target.value;
                    updateSelectedNode({
                      node_kind: nextKind,
                      label: defaultLabelForKind(nextKind)
                    });
                  }}
                >
                  <option value="trigger">trigger</option>
                  <option value="wait">wait</option>
                  <option value="cache_lookup">cache_lookup</option>
                  <option value="http_call">http_call</option>
                  <option value="condition">condition</option>
                  <option value="action">action</option>
                </select>
              </label>

              <label>
                Label
                <input
                  value={selectedData.label}
                  onChange={(e) => updateSelectedNode({ label: e.target.value })}
                />
              </label>

              {selectedData.node_kind === 'trigger' && (
                <label>
                  event_type
                  <select
                    value={selectedData.event_type || ''}
                    onChange={(e) => updateSelectedNode({ event_type: e.target.value })}
                  >
                    <option value="">Sec...</option>
                    {activeEventTypeOptions.map((eventType) => (
                      <option key={eventType} value={eventType}>
                        {eventType}
                      </option>
                    ))}
                    {!activeEventTypeOptions.includes(String(selectedData.event_type || '')) &&
                      selectedData.event_type && (
                        <option value={selectedData.event_type}>{selectedData.event_type}</option>
                      )}
                  </select>
                </label>
              )}

              {selectedData.node_kind === 'wait' && (
                <>
                  <label>
                    wait_minutes
                    <input
                      type="number"
                      min="1"
                      value={selectedData.wait_minutes}
                      onChange={(e) =>
                        updateSelectedNode({ wait_minutes: Math.max(1, Number(e.target.value) || 1) })
                      }
                    />
                  </label>
                  <label>
                    <input
                      type="checkbox"
                      checked={Boolean(selectedData.manual_release)}
                      onChange={(e) => updateSelectedNode({ manual_release: e.target.checked })}
                    />
                    manual_release (otomatik gecmesin)
                  </label>
                </>
              )}

              {selectedData.node_kind === 'http_call' && (
                <>
                  <label>
                    endpoint_id
                    <select
                      value={selectedData.endpoint_id || ''}
                      onChange={(e) => {
                        const nextEndpointId = e.target.value;
                        const matched = activeEndpointOptions.find(
                          (item) => String(item.endpoint_id) === String(nextEndpointId)
                        );
                        if (!matched) {
                          updateSelectedNode({ endpoint_id: nextEndpointId });
                          return;
                        }
                        updateSelectedNode({
                          endpoint_id: nextEndpointId,
                          http_method: String(matched.method || 'POST'),
                          http_url: String(matched.url || ''),
                          http_headers_json: JSON.stringify(matched.headers || {}, null, 2),
                          http_timeout_ms: Math.max(100, Number(matched.timeout_ms) || 5000)
                        });
                      }}
                    >
                      <option value="">Sec...</option>
                      {activeEndpointOptions.map((item) => (
                        <option key={item.endpoint_id} value={item.endpoint_id}>
                          {item.endpoint_id}
                        </option>
                      ))}
                      {!activeEndpointOptions.some(
                        (item) => String(item.endpoint_id) === String(selectedData.endpoint_id || '')
                      ) &&
                        selectedData.endpoint_id && (
                          <option value={selectedData.endpoint_id}>{selectedData.endpoint_id}</option>
                        )}
                    </select>
                  </label>
                  <label>
                    http_method
                    <select
                      value={selectedData.http_method}
                      onChange={(e) => updateSelectedNode({ http_method: e.target.value })}
                    >
                      <option value="GET">GET</option>
                      <option value="POST">POST</option>
                      <option value="PUT">PUT</option>
                      <option value="PATCH">PATCH</option>
                      <option value="DELETE">DELETE</option>
                    </select>
                  </label>
                  <label>
                    http_url
                    <input
                      placeholder="https://api.example.com/score"
                      value={selectedData.http_url}
                      onChange={(e) => updateSelectedNode({ http_url: e.target.value })}
                    />
                  </label>
                  <label>
                    http_headers_json
                    <textarea
                      value={selectedData.http_headers_json}
                      onChange={(e) => updateSelectedNode({ http_headers_json: e.target.value })}
                    />
                    <button
                      type="button"
                      onClick={() => prettifySelectedNodeJsonField('http_headers_json')}
                    >
                      Prettify Headers JSON
                    </button>
                    {!headersValidation.ok && (
                      <small className="fieldError">JSON hatasi: {headersValidation.message}</small>
                    )}
                    {headersValidation.ok && (
                      <small className="fieldSuccess">JSON valid</small>
                    )}
                  </label>
                  <label>
                    http_body_template
                    <input
                      value={selectedData.http_body_template}
                      onChange={(e) => updateSelectedNode({ http_body_template: e.target.value })}
                    />
                  </label>
                  <label>
                    http_timeout_ms
                    <input
                      type="number"
                      min="100"
                      value={selectedData.http_timeout_ms}
                      onChange={(e) =>
                        updateSelectedNode({
                          http_timeout_ms: Math.max(100, Number(e.target.value) || 100)
                        })
                      }
                    />
                  </label>
                  <label>
                    response_mapping_json
                    <textarea
                      value={selectedData.response_mapping_json}
                      onChange={(e) =>
                        updateSelectedNode({ response_mapping_json: e.target.value })
                      }
                    />
                    <button
                      type="button"
                      onClick={() => prettifySelectedNodeJsonField('response_mapping_json')}
                    >
                      Prettify Mapping JSON
                    </button>
                    <small className="fieldHelp">
                      Ornek: <code>{'{"score":"response.score","risk.level":"response.risk.level"}'}</code>
                    </small>
                    {!mappingValidation.ok && (
                      <small className="fieldError">JSON hatasi: {mappingValidation.message}</small>
                    )}
                    {mappingValidation.ok && (
                      <small className="fieldSuccess">JSON valid</small>
                    )}
                  </label>
                </>
              )}

              {selectedData.node_kind === 'cache_lookup' && (
                <>
                  <label>
                    cache_dataset_key
                    <select
                      value={selectedData.cache_dataset_key || ''}
                      onChange={(e) =>
                        updateSelectedNode({ cache_dataset_key: e.target.value })
                      }
                    >
                      <option value="">Sec...</option>
                      {activeCacheDatasetOptions.map((datasetKey) => (
                        <option key={datasetKey} value={datasetKey}>
                          {datasetKey}
                        </option>
                      ))}
                      {!activeCacheDatasetOptions.includes(
                        String(selectedData.cache_dataset_key || '')
                      ) &&
                        selectedData.cache_dataset_key && (
                          <option value={selectedData.cache_dataset_key}>
                            {selectedData.cache_dataset_key}
                          </option>
                        )}
                    </select>
                  </label>
                  <label>
                    cache_lookup_key_template
                    <input
                      placeholder="{{payload.merchant_id}}"
                      value={selectedData.cache_lookup_key_template}
                      onChange={(e) =>
                        updateSelectedNode({ cache_lookup_key_template: e.target.value })
                      }
                    />
                  </label>
                  <label>
                    cache_target_path
                    <input
                      placeholder="external.cache.item"
                      value={selectedData.cache_target_path}
                      onChange={(e) =>
                        updateSelectedNode({ cache_target_path: e.target.value })
                      }
                    />
                  </label>
                  <label>
                    cache_on_miss
                    <select
                      value={selectedData.cache_on_miss}
                      onChange={(e) => updateSelectedNode({ cache_on_miss: e.target.value })}
                    >
                      <option value="continue">continue</option>
                      <option value="default">default</option>
                      <option value="fail">fail</option>
                    </select>
                  </label>
                  {selectedData.cache_on_miss === 'default' && (
                    <label>
                      cache_default_json
                      <textarea
                        value={selectedData.cache_default_json}
                        onChange={(e) =>
                          updateSelectedNode({ cache_default_json: e.target.value })
                        }
                      />
                    </label>
                  )}
                </>
              )}

              {selectedData.node_kind === 'condition' && (
                <>
                  <label>
                    condition_key
                    <select
                      value={selectedData.condition_key}
                      onChange={(e) => updateSelectedNode({ condition_key: e.target.value })}
                    >
                      <option value="purchase_exists">purchase_exists</option>
                      <option value="event_exists">event_exists</option>
                      <option value="segment_match">segment_match</option>
                    </select>
                  </label>
                  {(selectedData.condition_key === 'purchase_exists' ||
                    selectedData.condition_key === 'event_exists') && (
                    <label>
                      condition_event_type
                      <select
                        value={selectedData.condition_event_type || ''}
                        onChange={(e) =>
                          updateSelectedNode({ condition_event_type: e.target.value })
                        }
                      >
                        <option value="">Sec...</option>
                        {activeEventTypeOptions.map((eventType) => (
                          <option key={eventType} value={eventType}>
                            {eventType}
                          </option>
                        ))}
                        {!activeEventTypeOptions.includes(String(selectedData.condition_event_type || '')) &&
                          selectedData.condition_event_type && (
                            <option value={selectedData.condition_event_type}>
                              {selectedData.condition_event_type}
                            </option>
                          )}
                      </select>
                    </label>
                  )}
                  {selectedData.condition_key === 'segment_match' && (
                    <label>
                      condition_segment_value
                      <select
                        value={selectedData.condition_segment_value || ''}
                        onChange={(e) =>
                          updateSelectedNode({ condition_segment_value: e.target.value })
                        }
                      >
                        <option value="">Sec...</option>
                        {activeSegmentOptions.map((segment) => (
                          <option key={segment.segment_key} value={segment.segment_key}>
                            {segment.segment_key}
                          </option>
                        ))}
                        {!activeSegmentOptions.some(
                          (segment) =>
                            String(segment.segment_key) ===
                            String(selectedData.condition_segment_value || '')
                        ) &&
                          selectedData.condition_segment_value && (
                            <option value={selectedData.condition_segment_value}>
                              {selectedData.condition_segment_value}
                            </option>
                          )}
                      </select>
                    </label>
                  )}
                </>
              )}

              {selectedData.node_kind === 'action' && (
                <>
                  <label>
                    channel
                    <select
                      value={selectedData.channel}
                      onChange={(e) => updateSelectedNode({ channel: e.target.value })}
                    >
                      <option value="email">email</option>
                      <option value="sms">sms</option>
                      <option value="push">push</option>
                    </select>
                  </label>

                  <label>
                    template_id
                    <select
                      value={selectedData.template_id || ''}
                      onChange={(e) => {
                        const nextTemplateId = e.target.value;
                        const matched = activeTemplateOptions.find(
                          (item) => String(item.template_id) === String(nextTemplateId)
                        );
                        if (!matched) {
                          updateSelectedNode({ template_id: nextTemplateId });
                          return;
                        }
                        updateSelectedNode({
                          template_id: nextTemplateId,
                          channel: String(matched.channel || selectedData.channel || 'email')
                        });
                      }}
                    >
                      <option value="">Sec...</option>
                      {activeTemplateOptions.map((item) => (
                        <option key={item.template_id} value={item.template_id}>
                          {item.template_id}
                        </option>
                      ))}
                      {!activeTemplateOptions.some(
                        (item) => String(item.template_id) === String(selectedData.template_id || '')
                      ) &&
                        selectedData.template_id && (
                          <option value={selectedData.template_id}>{selectedData.template_id}</option>
                        )}
                    </select>
                  </label>
                </>
              )}
            </div>
          )}

          {selectedEdge && (
            <div className="panelFields">
              <label>
                Edge ID
                <input value={selectedEdge.id} readOnly />
              </label>
              <label>
                edge_type
                <select
                  value={selectedEdge?.data?.edge_type || 'always'}
                  onChange={(e) => updateSelectedEdge({ edge_type: e.target.value })}
                >
                  <option value="always">always</option>
                  <option value="true">true</option>
                  <option value="false">false</option>
                  <option value="timeout">timeout</option>
                  <option value="error">error</option>
                </select>
              </label>
              <label>
                priority
                <input
                  type="number"
                  value={selectedEdge?.data?.priority ?? 100}
                  onChange={(e) =>
                    updateSelectedEdge({ priority: Math.max(0, Number(e.target.value) || 0) })
                  }
                />
              </label>
              <label>
                delay_minutes
                <input
                  type="number"
                  min="0"
                  value={selectedEdge?.data?.delay_minutes ?? 0}
                  onChange={(e) =>
                    updateSelectedEdge({
                      delay_minutes: Math.max(0, Number(e.target.value) || 0)
                    })
                  }
                />
              </label>
              <label>
                expression
                <input
                  placeholder="payload.amount > 1000"
                  value={selectedEdge?.data?.expression || ''}
                  onChange={(e) => updateSelectedEdge({ expression: e.target.value })}
                />
                <small className="fieldHelp">
                  Ornek: <code>exists(payload.amount)</code>, <code>{'external.http.normalized.score >= 700'}</code>, <code>external.http.response.message contains "approved"</code>
                </small>
              </label>
              <div className="expressionBuilder">
                <div className="expressionBuilderHead">Expression Builder (Cache)</div>
                {activeCacheDatasetOptions.length === 0 && (
                  <small className="fieldHelp">
                    Cache dataset bulunamadi. Once Catalogues &gt; Cache Loader ile dataset yukleyin.
                  </small>
                )}
                {activeCacheDatasetOptions.length > 0 && (
                  <>
                    <label>
                      dataset
                      <select
                        value={exprBuilderDatasetKey}
                        onChange={(e) => setExprBuilderDatasetKey(e.target.value)}
                      >
                        <option value="">Sec...</option>
                        {activeCacheDatasetOptions.map((datasetKey) => (
                          <option key={datasetKey} value={datasetKey}>
                            {datasetKey}
                          </option>
                        ))}
                      </select>
                    </label>
                    <label>
                      column
                      <select
                        value={exprBuilderColumn}
                        onChange={(e) => setExprBuilderColumn(e.target.value)}
                      >
                        <option value="">Sec...</option>
                        {selectedBuilderDatasetColumns.map((column) => (
                          <option key={column} value={column}>
                            {column}
                          </option>
                        ))}
                      </select>
                    </label>
                    <label>
                      operator
                      <select
                        value={exprBuilderOperator}
                        onChange={(e) => setExprBuilderOperator(e.target.value)}
                      >
                        {availableExprOperators.map((op) => (
                          <option key={op} value={op}>
                            {op}
                          </option>
                        ))}
                      </select>
                    </label>
                    <small className="fieldHelp">
                      Kolon tipi: <code>{selectedBuilderColumnType}</code>
                    </small>
                    <label>
                      value_type
                      <select
                        value={exprBuilderValueType}
                        onChange={(e) => setExprBuilderValueType(e.target.value)}
                      >
                        <option value="payload">payload</option>
                        <option value="attributes">attributes</option>
                        <option value="static">static</option>
                      </select>
                    </label>
                    <label>
                      {exprBuilderValueType === 'static' ? 'static_value' : `${exprBuilderValueType}_path`}
                      <input
                        placeholder={
                          exprBuilderValueType === 'static'
                            ? '1000 veya approved'
                            : exprBuilderColumn || 'amount'
                        }
                        value={exprBuilderValueInput}
                        onChange={(e) => setExprBuilderValueInput(e.target.value)}
                      />
                    </label>
                    <div className="expressionBuilderActions">
                      <button type="button" onClick={buildEdgeExpression}>
                        Expression Olustur
                      </button>
                    </div>
                    <small className="fieldHelp">
                      Sol taraf otomatik: <code>{builderCacheTargetPath}.{exprBuilderColumn || 'column'}</code>
                    </small>
                  </>
                )}
              </div>
              <label>
                rate_limit_per_day
                <input
                  type="number"
                  min="0"
                  value={selectedEdge?.data?.rate_limit_per_day ?? 0}
                  onChange={(e) =>
                    updateSelectedEdge({
                      rate_limit_per_day: Math.max(0, Number(e.target.value) || 0)
                    })
                  }
                />
              </label>
              <label>
                max_customers_total
                <input
                  type="number"
                  min="0"
                  value={selectedEdge?.data?.max_customers_total ?? 0}
                  onChange={(e) =>
                    updateSelectedEdge({
                      max_customers_total: Math.max(0, Number(e.target.value) || 0)
                    })
                  }
                />
              </label>
              <label>
                max_customers_per_day
                <input
                  type="number"
                  min="0"
                  value={selectedEdge?.data?.max_customers_per_day ?? 0}
                  onChange={(e) =>
                    updateSelectedEdge({
                      max_customers_per_day: Math.max(0, Number(e.target.value) || 0)
                    })
                  }
                />
              </label>
            </div>
          )}
        </aside>
        )}
      </main>
      {showJourneyLogs && (
      <section className="journeyLogsPanel">
        <div className="journeyLogsHead">
          <h3>Journey Event Log</h3>
          <span>{journeyId} v{version} | {journeyLogsWindowLabel}</span>
        </div>
        <div className="journeyLogsTableWrap">
          <table className="journeyLogsTable">
            <thead>
              <tr>
                <th>Musteri Numarasi</th>
                <th>Log Tarihi</th>
                <th>Onceki State</th>
                <th>Mevcut State</th>
                <th>Reason</th>
              </tr>
            </thead>
            <tbody>
              {journeyLogsLoading && (
                <tr>
                  <td colSpan={5}>Yukleniyor...</td>
                </tr>
              )}
              {!journeyLogsLoading && journeyLogs.length === 0 && (
                <tr>
                  <td colSpan={5}>Bu journey icin log bulunamadi.</td>
                </tr>
              )}
              {!journeyLogsLoading &&
                journeyLogs.map((item) => (
                  <tr key={item.id}>
                    <td>{item.customer_id}</td>
                    <td>{formatLogDate(item.created_at)}</td>
                    <td>{item.from_state || '-'}</td>
                    <td>{item.to_state}</td>
                    <td>{item.reason || '-'}</td>
                  </tr>
                ))}
            </tbody>
          </table>
        </div>
      </section>
      )}
      {showManualQueue && (
      <section className="journeyLogsPanel">
        <div className="journeyLogsHead">
          <h3>Manual Wait Queue</h3>
          <span>{journeyId} v{version}</span>
        </div>
        <div className="manualQueueToolbar">
          <label>
            Wait Node
            <select
              value={manualWaitNodeId}
              onChange={(e) => setManualWaitNodeId(e.target.value)}
            >
              {waitNodes.map((node) => (
                <option key={node.id} value={node.id}>
                  {node.id}
                </option>
              ))}
            </select>
          </label>
          <label>
            Release Count
            <input
              type="number"
              min="1"
              value={manualReleaseCount}
              onChange={(e) => setManualReleaseCount(Math.max(1, Number(e.target.value) || 1))}
            />
          </label>
          <button
            type="button"
            onClick={fetchManualQueue}
            disabled={manualQueueLoading || !manualWaitNodeId}
          >
            {manualQueueLoading ? 'Loading...' : 'Load Queue'}
          </button>
          <button
            type="button"
            className="primary"
            onClick={releaseManualQueue}
            disabled={manualQueueLoading || !manualWaitNodeId}
          >
            Release N
          </button>
        </div>
        <div className="journeyLogsTableWrap">
          <table className="journeyLogsTable">
            <thead>
              <tr>
                <th>Instance</th>
                <th>Musteri</th>
                <th>State</th>
                <th>Started At</th>
                <th>Updated At</th>
              </tr>
            </thead>
            <tbody>
              {manualQueueLoading && (
                <tr>
                  <td colSpan={5}>Yukleniyor...</td>
                </tr>
              )}
              {!manualQueueLoading && manualQueueItems.length === 0 && (
                <tr>
                  <td colSpan={5}>Manual queue bos.</td>
                </tr>
              )}
              {!manualQueueLoading &&
                manualQueueItems.map((item) => (
                  <tr key={item.instance_id}>
                    <td>{item.instance_id}</td>
                    <td>{item.customer_id}</td>
                    <td>{item.state}</td>
                    <td>{formatLogDate(item.started_at)}</td>
                    <td>{formatLogDate(item.updated_at)}</td>
                  </tr>
                ))}
            </tbody>
          </table>
        </div>
      </section>
      )}
      </>
      )}

      {activeMenu === 'Scenarios' && viewMode === 'library' && (
        <main className="libraryWorkspace">
          <section className="libraryToolbar">
            <button type="button" className="primary" onClick={newJourney}>
              + New Journey
            </button>
            <div className="librarySummary">
              <strong>{selectedFolder}</strong>
              <span>{journeysInSelectedFolder.length} journey</span>
            </div>
            <label>
              Klasor sec
              <select value={selectedFolder} onChange={(e) => setSelectedFolder(e.target.value)}>
                {allFolders.map((folder) => (
                  <option key={folder} value={folder}>
                    {folder}
                  </option>
                ))}
              </select>
            </label>
            <label>
              Yeni klasor
              <input
                value={newFolderName}
                onChange={(e) => setNewFolderName(e.target.value)}
                placeholder="or: Test Senaryolari"
              />
            </label>
            <button type="button" onClick={createFolder} disabled={busy}>
              Klasor Olustur
            </button>
          </section>

          <section className="folderGrid">
            {allFolders.map((folder) => (
              <div
                key={folder}
                role="button"
                tabIndex={0}
                className={
                  folder === selectedFolder
                    ? activeDropFolder === folder
                      ? 'folderCard active dropActive'
                      : 'folderCard active'
                    : activeDropFolder === folder
                      ? 'folderCard dropActive'
                      : 'folderCard'
                }
                onClick={() => setSelectedFolder(folder)}
                onDragEnter={(e) => onFolderDragEnter(e, folder)}
                onDragOver={(e) => onFolderDragOver(e, folder)}
                onDrop={(e) => onFolderDrop(e, folder)}
              >
                <div className="folderHead">
                  <span className="folderIcon">[DIR]</span>
                  <span className="folderCount">{folderCounts[folder] || 0}</span>
                </div>
                <strong className="folderName">{folder}</strong>
                <small className="folderHint">
                  {activeDropFolder === folder ? 'Drop now' : 'Drop journey here'}
                </small>
              </div>
            ))}
          </section>

          <section className="journeyGrid">
            {journeysInSelectedFolder.map((item) => (
              <div
                key={`${item.journey_id}::${item.version}`}
                role="button"
                tabIndex={0}
                draggable
                className={
                  draggedJourneyKey === `${item.journey_id}::${item.version}`
                    ? 'journeyCard dragging'
                    : 'journeyCard'
                }
                onClick={() => applyJourneyToCanvas(item)}
                onDragStart={(e) => onJourneyDragStart(e, item)}
                onDragEnd={onJourneyDragEnd}
              >
                <strong>{item.name || item.journey_id}</strong>
                <span>{item.journey_id} v{item.version}</span>
                <span>{item.status}</span>
                <small>Drag to folder to move</small>
              </div>
            ))}
            {journeysInSelectedFolder.length === 0 && (
              <p className="panelHint">Bu klasorde journey yok.</p>
            )}
          </section>
          <div className="statusBar">{statusText}</div>
        </main>
      )}
      {activeMenu === 'Catalogues' && (
        <main className="dashboardWorkspace">
          <section className="dashboardHeader">
            <h2>Catalogues</h2>
            <button type="button" onClick={fetchCatalogues} disabled={cataloguesLoading}>
              {cataloguesLoading ? 'Yukleniyor...' : 'Yenile'}
            </button>
          </section>

          <section className="dashboardGrid">
            <article className="kpiCard">
              <h3>Event Types</h3>
              <div className="kpiValue">{catalogueSummary?.event_types?.total ?? 0}</div>
              <small>Aktif: {catalogueSummary?.event_types?.active_total ?? 0}</small>
            </article>
            <article className="kpiCard">
              <h3>Templates</h3>
              <div className="kpiValue">{catalogueSummary?.templates?.total ?? 0}</div>
              <small>Aktif: {catalogueSummary?.templates?.active_total ?? 0}</small>
            </article>
            <article className="kpiCard">
              <h3>Endpoints</h3>
              <div className="kpiValue">{catalogueSummary?.endpoints?.total ?? 0}</div>
              <small>Aktif: {catalogueSummary?.endpoints?.active_total ?? 0}</small>
            </article>
            <article className="kpiCard">
              <h3>Segments</h3>
              <div className="kpiValue">{catalogueSummary?.segments?.total ?? 0}</div>
              <small>Aktif: {catalogueSummary?.segments?.active_total ?? 0}</small>
            </article>
          </section>

          <section className="catalogueSection">
            <article className="kpiCard">
              <h3>Event Type Catalogue</h3>
              <div className="catalogueFormGrid">
                <label>
                  event_type
                  <input
                    value={eventTypeForm.event_type}
                    onChange={(e) =>
                      setEventTypeForm((current) => ({ ...current, event_type: e.target.value }))
                    }
                  />
                </label>
                <label>
                  description
                  <input
                    value={eventTypeForm.description}
                    onChange={(e) =>
                      setEventTypeForm((current) => ({ ...current, description: e.target.value }))
                    }
                  />
                </label>
                <label>
                  owner
                  <input
                    value={eventTypeForm.owner}
                    onChange={(e) =>
                      setEventTypeForm((current) => ({ ...current, owner: e.target.value }))
                    }
                  />
                </label>
                <label>
                  version
                  <input
                    type="number"
                    min="1"
                    value={eventTypeForm.version}
                    onChange={(e) =>
                      setEventTypeForm((current) => ({
                        ...current,
                        version: Math.max(1, Number(e.target.value) || 1)
                      }))
                    }
                  />
                </label>
                <label className="wide">
                  required_fields (csv)
                  <input
                    value={eventTypeForm.required_fields_csv}
                    onChange={(e) =>
                      setEventTypeForm((current) => ({
                        ...current,
                        required_fields_csv: e.target.value
                      }))
                    }
                    placeholder="customer_id, payload.email, payload.amount"
                  />
                </label>
                <label className="wide">
                  schema_json (json)
                  <textarea
                    value={eventTypeForm.schema_json}
                    onChange={(e) =>
                      setEventTypeForm((current) => ({ ...current, schema_json: e.target.value }))
                    }
                  />
                </label>
                <label className="wide">
                  sample_payload (json)
                  <textarea
                    value={eventTypeForm.sample_payload}
                    onChange={(e) =>
                      setEventTypeForm((current) => ({ ...current, sample_payload: e.target.value }))
                    }
                  />
                </label>
                <label>
                  <input
                    type="checkbox"
                    checked={Boolean(eventTypeForm.is_active)}
                    onChange={(e) =>
                      setEventTypeForm((current) => ({ ...current, is_active: e.target.checked }))
                    }
                  />
                  active
                </label>
                <button type="button" className="primary" onClick={saveEventType} disabled={cataloguesLoading}>
                  Kaydet
                </button>
              </div>
              <div className="dashboardTableWrap">
                <table className="journeyLogsTable">
                  <thead>
                    <tr>
                      <th>event_type</th>
                      <th>owner</th>
                      <th>version</th>
                      <th>required_fields</th>
                      <th>description</th>
                      <th>active</th>
                      <th>islem</th>
                    </tr>
                  </thead>
                  <tbody>
                    {catalogueEventTypes.map((item) => (
                      <tr key={item.event_type}>
                        <td>{item.event_type}</td>
                        <td>{item.owner || '-'}</td>
                        <td>{item.version || 1}</td>
                        <td>{Array.isArray(item.required_fields) ? item.required_fields.join(', ') : '-'}</td>
                        <td>{item.description}</td>
                        <td>{item.is_active ? 'true' : 'false'}</td>
                        <td>
                          <button
                            type="button"
                            onClick={() => deleteCatalogueItem('event_type', item.event_type)}
                            disabled={cataloguesLoading}
                          >
                            Sil
                          </button>
                        </td>
                      </tr>
                    ))}
                    {catalogueEventTypes.length === 0 && (
                      <tr>
                        <td colSpan={7}>Event type yok.</td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </article>

            <article className="kpiCard">
              <h3>Segment Catalogue</h3>
              <div className="catalogueFormGrid">
                <label>
                  segment_key
                  <input
                    value={segmentForm.segment_key}
                    onChange={(e) =>
                      setSegmentForm((current) => ({ ...current, segment_key: e.target.value }))
                    }
                    placeholder="vip, new_user, risk_high"
                  />
                </label>
                <label>
                  display_name
                  <input
                    value={segmentForm.display_name}
                    onChange={(e) =>
                      setSegmentForm((current) => ({ ...current, display_name: e.target.value }))
                    }
                  />
                </label>
                <label className="wide">
                  rule_expression
                  <input
                    value={segmentForm.rule_expression}
                    onChange={(e) =>
                      setSegmentForm((current) => ({
                        ...current,
                        rule_expression: e.target.value
                      }))
                    }
                    placeholder="attributes.tier >= 3 && attributes.risk != 'high'"
                  />
                </label>
                <label className="wide">
                  description
                  <input
                    value={segmentForm.description}
                    onChange={(e) =>
                      setSegmentForm((current) => ({ ...current, description: e.target.value }))
                    }
                  />
                </label>
                <label>
                  <input
                    type="checkbox"
                    checked={Boolean(segmentForm.is_active)}
                    onChange={(e) =>
                      setSegmentForm((current) => ({ ...current, is_active: e.target.checked }))
                    }
                  />
                  active
                </label>
                <button type="button" className="primary" onClick={saveSegment} disabled={cataloguesLoading}>
                  Kaydet
                </button>
              </div>
              <div className="dashboardTableWrap">
                <table className="journeyLogsTable">
                  <thead>
                    <tr>
                      <th>segment_key</th>
                      <th>display_name</th>
                      <th>rule_expression</th>
                      <th>description</th>
                      <th>active</th>
                      <th>islem</th>
                    </tr>
                  </thead>
                  <tbody>
                    {catalogueSegments.map((item) => (
                      <tr key={item.segment_key}>
                        <td>{item.segment_key}</td>
                        <td>{item.display_name}</td>
                        <td>{item.rule_expression}</td>
                        <td>{item.description}</td>
                        <td>{item.is_active ? 'true' : 'false'}</td>
                        <td>
                          <button
                            type="button"
                            onClick={() => deleteCatalogueItem('segment', item.segment_key)}
                            disabled={cataloguesLoading}
                          >
                            Sil
                          </button>
                        </td>
                      </tr>
                    ))}
                    {catalogueSegments.length === 0 && (
                      <tr>
                        <td colSpan={6}>Segment yok.</td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </article>

            <article className="kpiCard">
              <h3>Template Catalogue</h3>
              <div className="catalogueFormGrid">
                <label>
                  template_id
                  <input
                    value={templateForm.template_id}
                    onChange={(e) =>
                      setTemplateForm((current) => ({ ...current, template_id: e.target.value }))
                    }
                  />
                </label>
                <label>
                  channel
                  <select
                    value={templateForm.channel}
                    onChange={(e) =>
                      setTemplateForm((current) => ({ ...current, channel: e.target.value }))
                    }
                  >
                    <option value="email">email</option>
                    <option value="sms">sms</option>
                    <option value="push">push</option>
                  </select>
                </label>
                <label>
                  subject
                  <input
                    value={templateForm.subject}
                    onChange={(e) =>
                      setTemplateForm((current) => ({ ...current, subject: e.target.value }))
                    }
                  />
                </label>
                <label>
                  variables (csv)
                  <input
                    value={templateForm.variables_csv}
                    onChange={(e) =>
                      setTemplateForm((current) => ({ ...current, variables_csv: e.target.value }))
                    }
                    placeholder="name,amount,segment"
                  />
                </label>
                <label className="wide">
                  body
                  <textarea
                    value={templateForm.body}
                    onChange={(e) =>
                      setTemplateForm((current) => ({ ...current, body: e.target.value }))
                    }
                  />
                </label>
                <label>
                  <input
                    type="checkbox"
                    checked={Boolean(templateForm.is_active)}
                    onChange={(e) =>
                      setTemplateForm((current) => ({ ...current, is_active: e.target.checked }))
                    }
                  />
                  active
                </label>
                <button type="button" className="primary" onClick={saveTemplate} disabled={cataloguesLoading}>
                  Kaydet
                </button>
              </div>
              <div className="dashboardTableWrap">
                <table className="journeyLogsTable">
                  <thead>
                    <tr>
                      <th>template_id</th>
                      <th>channel</th>
                      <th>subject</th>
                      <th>active</th>
                      <th>islem</th>
                    </tr>
                  </thead>
                  <tbody>
                    {catalogueTemplates.map((item) => (
                      <tr key={item.template_id}>
                        <td>{item.template_id}</td>
                        <td>{item.channel}</td>
                        <td>{item.subject}</td>
                        <td>{item.is_active ? 'true' : 'false'}</td>
                        <td>
                          <button
                            type="button"
                            onClick={() => deleteCatalogueItem('template', item.template_id)}
                            disabled={cataloguesLoading}
                          >
                            Sil
                          </button>
                        </td>
                      </tr>
                    ))}
                    {catalogueTemplates.length === 0 && (
                      <tr>
                        <td colSpan={5}>Template yok.</td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </article>

            <article className="kpiCard">
              <h3>Endpoint Catalogue</h3>
              <div className="catalogueFormGrid">
                <label>
                  endpoint_id
                  <input
                    value={endpointForm.endpoint_id}
                    onChange={(e) =>
                      setEndpointForm((current) => ({ ...current, endpoint_id: e.target.value }))
                    }
                  />
                </label>
                <label>
                  method
                  <select
                    value={endpointForm.method}
                    onChange={(e) =>
                      setEndpointForm((current) => ({ ...current, method: e.target.value }))
                    }
                  >
                    <option value="GET">GET</option>
                    <option value="POST">POST</option>
                    <option value="PUT">PUT</option>
                    <option value="PATCH">PATCH</option>
                    <option value="DELETE">DELETE</option>
                  </select>
                </label>
                <label>
                  timeout_ms
                  <input
                    type="number"
                    min="100"
                    value={endpointForm.timeout_ms}
                    onChange={(e) =>
                      setEndpointForm((current) => ({
                        ...current,
                        timeout_ms: Math.max(100, Number(e.target.value) || 100)
                      }))
                    }
                  />
                </label>
                <label className="wide">
                  url
                  <input
                    value={endpointForm.url}
                    onChange={(e) =>
                      setEndpointForm((current) => ({ ...current, url: e.target.value }))
                    }
                  />
                </label>
                <label className="wide">
                  headers (json)
                  <textarea
                    value={endpointForm.headers}
                    onChange={(e) =>
                      setEndpointForm((current) => ({ ...current, headers: e.target.value }))
                    }
                  />
                </label>
                <label className="wide">
                  description
                  <input
                    value={endpointForm.description}
                    onChange={(e) =>
                      setEndpointForm((current) => ({ ...current, description: e.target.value }))
                    }
                  />
                </label>
                <label>
                  <input
                    type="checkbox"
                    checked={Boolean(endpointForm.is_active)}
                    onChange={(e) =>
                      setEndpointForm((current) => ({ ...current, is_active: e.target.checked }))
                    }
                  />
                  active
                </label>
                <button type="button" className="primary" onClick={saveEndpoint} disabled={cataloguesLoading}>
                  Kaydet
                </button>
              </div>
              <div className="dashboardTableWrap">
                <table className="journeyLogsTable">
                  <thead>
                    <tr>
                      <th>endpoint_id</th>
                      <th>method</th>
                      <th>url</th>
                      <th>active</th>
                      <th>islem</th>
                    </tr>
                  </thead>
                  <tbody>
                    {catalogueEndpoints.map((item) => (
                      <tr key={item.endpoint_id}>
                        <td>{item.endpoint_id}</td>
                        <td>{item.method}</td>
                        <td>{item.url}</td>
                        <td>{item.is_active ? 'true' : 'false'}</td>
                        <td>
                          <button
                            type="button"
                            onClick={() => deleteCatalogueItem('endpoint', item.endpoint_id)}
                            disabled={cataloguesLoading}
                          >
                            Sil
                          </button>
                        </td>
                      </tr>
                    ))}
                    {catalogueEndpoints.length === 0 && (
                      <tr>
                        <td colSpan={5}>Endpoint yok.</td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </article>
          </section>
        </main>
      )}
      {activeMenu === 'Dashboards' && (
        <main className="dashboardWorkspace">
          <section className="dashboardHeader dashboardHero">
            <div>
              <h2>Genel Durum KPI</h2>
              <small>Operasyon gorunumu, son 24 saat performans ozeti</small>
            </div>
            <button
              type="button"
              onClick={async () => {
                await Promise.all([
                  fetchDashboardKpi(),
                  fetchDashboardJourneyPerformance(),
                  fetchDashboardCacheHealth()
                ]);
              }}
              disabled={dashboardLoading}
            >
              {dashboardLoading ? 'Yukleniyor...' : 'Yenile'}
            </button>
          </section>
          <section className="dashboardMetaBar">
            <span className="dashboardChip">Veri araligi: 1s / 24s</span>
            <span className="dashboardChip">Son yenileme: {dashboardLastUpdated || '-'}</span>
          </section>
          <section className="dashboardGrid">
            <article className="kpiCard dashboardCard">
              <div className="dashboardCardHead">
                <span className="dashboardIcon">EV</span>
                <h3>Event Sayisi</h3>
              </div>
              <div className="kpiValue">{dashboardKpi?.events_1h ?? 0}</div>
              <small>Son 1 saat</small>
              <div className="kpiSub">24s: {dashboardKpi?.events_24h ?? 0}</div>
            </article>
            <article className="kpiCard dashboardCard">
              <div className="dashboardCardHead">
                <span className="dashboardIcon">IN</span>
                <h3>Aktif Instance</h3>
              </div>
              <div className="kpiValue">{dashboardKpi?.active_instances?.total ?? 0}</div>
              <small>waiting + waiting_manual + processing</small>
              <div className="kpiSub">
                w:{dashboardKpi?.active_instances?.waiting ?? 0} | wm:{dashboardKpi?.active_instances?.waiting_manual ?? 0} | p:{dashboardKpi?.active_instances?.processing ?? 0}
              </div>
            </article>
            <article className="kpiCard dashboardCard">
              <div className="dashboardCardHead">
                <span className="dashboardIcon">CM</span>
                <h3>Tamamlanan Journey</h3>
              </div>
              <div className="kpiValue">{dashboardKpi?.completed_journeys_24h ?? 0}</div>
              <small>Son 24 saat completed instance</small>
            </article>
            <article className="kpiCard dashboardCard">
              <div className="dashboardCardHead">
                <span className="dashboardIcon">AC</span>
                <h3>Action Orani (24s)</h3>
              </div>
              <div className="kpiValue">{dashboardKpi?.actions_24h?.success_rate_pct ?? 0}%</div>
              <small>Basarili oran</small>
              <div className="dashboardProgress">
                <span
                  className="dashboardProgressFill"
                  style={{ width: `${Math.max(0, Math.min(100, Number(dashboardKpi?.actions_24h?.success_rate_pct ?? 0)))}%` }}
                />
              </div>
              <div className="kpiSub">
                fail: {dashboardKpi?.actions_24h?.failure_rate_pct ?? 0}% | ok:{' '}
                {dashboardKpi?.actions_24h?.success ?? 0} / fail:{' '}
                {dashboardKpi?.actions_24h?.failed ?? 0}
              </div>
            </article>
          </section>
          <section className="dashboardGrid">
            <article className="kpiCard dashboardCard">
              <div className="dashboardCardHead">
                <span className="dashboardIcon">CH</span>
                <h3>Cache Dataset</h3>
              </div>
              <div className="kpiValue">{dashboardCacheHealth?.summary?.datasets_total ?? 0}</div>
              <small>Toplam tanimli dataset</small>
            </article>
            <article className="kpiCard dashboardCard">
              <div className="dashboardCardHead">
                <span className="dashboardIcon">TD</span>
                <h3>Bugun Yuklenen</h3>
              </div>
              <div className="kpiValue">{dashboardCacheHealth?.summary?.datasets_loaded_today ?? 0}</div>
              <small>Bugun en az 1 basarili yukleme alan dataset</small>
            </article>
            <article className="kpiCard dashboardCard">
              <div className="dashboardCardHead">
                <span className="dashboardIcon">RN</span>
                <h3>Bugun Basarili Run</h3>
              </div>
              <div className="kpiValue">{dashboardCacheHealth?.summary?.success_runs_today ?? 0}</div>
              <small>Cache loader bugun success run sayisi</small>
            </article>
            <article className="kpiCard dashboardCard">
              <div className="dashboardCardHead">
                <span className="dashboardIcon">RW</span>
                <h3>Son Satir Toplami</h3>
              </div>
              <div className="kpiValue">{dashboardCacheHealth?.summary?.total_rows_last_success ?? 0}</div>
              <small>Tum datasetlerin son basarili row_count toplami</small>
            </article>
          </section>
          <section className="dashboardSplit">
            <article className="kpiCard dashboardCard">
              <h3>En Cok Event Alan Journey</h3>
              <div className="dashboardList">
                {(dashboardJourneyPerf?.top_by_events || []).map((item, index) => (
                  <div key={`${item.journey_id}::${item.journey_version}`} className="dashboardListRow dashboardRankRow">
                    <span className="dashboardRank">{index + 1}</span>
                    <span className="dashboardRankName">{item.name}</span>
                    <strong>{item.triggered_24h}</strong>
                    <span
                      className="dashboardMiniBar"
                      style={{
                        width: `${Math.max(
                          12,
                          Math.round((Number(item.triggered_24h || 0) / topEventMax) * 100)
                        )}%`
                      }}
                    />
                  </div>
                ))}
                {(dashboardJourneyPerf?.top_by_events || []).length === 0 && (
                  <small>Veri yok.</small>
                )}
              </div>
            </article>
            <article className="kpiCard dashboardCard">
              <h3>En Cok Fail Ureten Journey</h3>
              <div className="dashboardList">
                {(dashboardJourneyPerf?.top_by_failures || []).map((item, index) => (
                  <div key={`${item.journey_id}::${item.journey_version}`} className="dashboardListRow dashboardRankRow fail">
                    <span className="dashboardRank">{index + 1}</span>
                    <span className="dashboardRankName">{item.name}</span>
                    <strong>{item.failed_actions_24h}</strong>
                    <span
                      className="dashboardMiniBar fail"
                      style={{
                        width: `${Math.max(
                          12,
                          Math.round((Number(item.failed_actions_24h || 0) / topFailMax) * 100)
                        )}%`
                      }}
                    />
                  </div>
                ))}
                {(dashboardJourneyPerf?.top_by_failures || []).length === 0 && (
                  <small>Veri yok.</small>
                )}
              </div>
            </article>
          </section>
          <section className="dashboardTableWrap dashboardTableCard">
            <div className="dashboardTableHead">
              <h3>Cache Health Detay</h3>
              <small>Dataset bazinda son yukleme durumu ve bugun yuklenme bilgisi</small>
            </div>
            <table className="journeyLogsTable">
              <thead>
                <tr>
                  <th>Dataset</th>
                  <th>Bugun Yuklendi mi</th>
                  <th>Son Basarili Yukleme</th>
                  <th>Son Basarili Satir</th>
                  <th>Son Run Status</th>
                  <th>Son Run Tarihi</th>
                </tr>
              </thead>
              <tbody>
                {(dashboardCacheHealth?.items || []).map((item) => (
                  <tr key={item.dataset_key}>
                    <td>{item.dataset_key}</td>
                    <td>{item.loaded_today ? 'Evet' : 'Hayir'}</td>
                    <td>{formatLogDate(item.last_success_at)}</td>
                    <td>{item.last_success_row_count ?? 0}</td>
                    <td>{item.last_run_status || '-'}</td>
                    <td>{formatLogDate(item.last_run_at)}</td>
                  </tr>
                ))}
                {(dashboardCacheHealth?.items || []).length === 0 && (
                  <tr>
                    <td colSpan={6}>Cache dataset verisi yok.</td>
                  </tr>
                )}
              </tbody>
            </table>
          </section>
          <section className="dashboardTableWrap dashboardTableCard">
            <div className="dashboardTableHead">
              <h3>Journey Bazli Performans</h3>
              <small>Tetiklenme, completion, fail ve ortalama bekleme suresi</small>
            </div>
            <table className="journeyLogsTable">
              <thead>
                <tr>
                  <th>Journey</th>
                  <th>Version</th>
                  <th>Tetiklenme (24s)</th>
                  <th>Completion (24s)</th>
                  <th>Fail (24s)</th>
                  <th>Ort. Bekleme</th>
                </tr>
              </thead>
              <tbody>
                {(dashboardJourneyPerf?.journeys || []).map((item) => (
                  <tr key={`${item.journey_id}::${item.journey_version}`}>
                    <td>{item.name}</td>
                    <td>{item.journey_version}</td>
                    <td>{item.triggered_24h}</td>
                    <td>{item.completed_24h}</td>
                    <td>{item.failed_actions_24h}</td>
                    <td>{formatDurationMinutes(item.avg_wait_seconds)}</td>
                  </tr>
                ))}
                {(dashboardJourneyPerf?.journeys || []).length === 0 && (
                  <tr>
                    <td colSpan={6}>Journey performans verisi yok.</td>
                  </tr>
                )}
              </tbody>
            </table>
          </section>
        </main>
      )}
      {activeMenu === 'Management' && (
        <main className="dashboardWorkspace">
          <section className="dashboardHeader">
            <h2>Release Control</h2>
            <button type="button" onClick={fetchManagementData} disabled={managementLoading}>
              {managementLoading ? 'Yukleniyor...' : 'Yenile'}
            </button>
          </section>

          <section className="kpiCard">
            <h3>Bakim Modu / Global Pause</h3>
            <div className="manualQueueToolbar">
              <label>
                <input
                  type="checkbox"
                  checked={globalPauseEnabled}
                  onChange={(e) => setGlobalPauseEnabled(e.target.checked)}
                />
                Global pause aktif
              </label>
              <button type="button" className="primary" onClick={saveGlobalPause} disabled={managementLoading}>
                Kaydet
              </button>
            </div>
            <small>Aktif olunca yeni eventler ve scheduler akisi durdurulur.</small>
          </section>

          <section className="dashboardTableWrap">
            <table className="journeyLogsTable">
              <thead>
                <tr>
                  <th>Journey</th>
                  <th>Version</th>
                  <th>Status</th>
                  <th>Trafik Orani (%)</th>
                  <th>Journey Pause</th>
                  <th>Kaydet</th>
                </tr>
              </thead>
              <tbody>
                {releaseControls.map((item) => (
                  <tr key={`${item.journey_id}::${item.journey_version}`}>
                    <td>{item.name || item.journey_id}</td>
                    <td>{item.journey_version}</td>
                    <td>{item.status}</td>
                    <td>
                      <select
                        value={Number(item.rollout_percent)}
                        onChange={(e) =>
                          setReleaseControls((current) =>
                            current.map((row) =>
                              row.journey_id === item.journey_id
                                ? { ...row, rollout_percent: Number(e.target.value) }
                                : row
                            )
                          )
                        }
                      >
                        <option value={10}>10%</option>
                        <option value={50}>50%</option>
                        <option value={100}>100%</option>
                      </select>
                    </td>
                    <td>
                      <input
                        type="checkbox"
                        checked={Boolean(item.release_paused)}
                        onChange={(e) =>
                          setReleaseControls((current) =>
                            current.map((row) =>
                              row.journey_id === item.journey_id
                                ? { ...row, release_paused: e.target.checked }
                                : row
                            )
                          )
                        }
                      />
                    </td>
                    <td>
                      <button
                        type="button"
                        onClick={() => saveReleaseControl(item.journey_id)}
                        disabled={managementLoading}
                      >
                        Kaydet
                      </button>
                    </td>
                  </tr>
                ))}
                {releaseControls.length === 0 && (
                  <tr>
                    <td colSpan={6}>Release control verisi yok.</td>
                  </tr>
                )}
              </tbody>
            </table>
          </section>
        </main>
      )}
      {(activeMenu === 'Dashboards' || activeMenu === 'Management' || activeMenu === 'Catalogues' || viewMode !== 'library') && <div className="statusBar">{statusText}</div>}
      </div>
    </div>
  );
}

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
