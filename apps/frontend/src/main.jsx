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
    http_method: data.http_method || 'POST',
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
    position: { x: 600, y: 120 },
    data: normalizeNodeData({ node_kind: 'http_call' }, 'http_call')
  },
  {
    id: 'condition',
    position: { x: 860, y: 120 },
    data: normalizeNodeData({ node_kind: 'condition' }, 'condition')
  },
  {
    id: 'action',
    position: { x: 1120, y: 120 },
    data: normalizeNodeData({ node_kind: 'action' }, 'action'),
    type: 'output'
  }
];

const defaultEdges = [
  { id: 'e1', source: 'trigger', target: 'wait', animated: true },
  { id: 'e2', source: 'wait', target: 'http_call' },
  { id: 'e3', source: 'http_call', target: 'condition' },
  { id: 'e4', source: 'condition', target: 'action' }
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
  const [selectedJourneyKey, setSelectedJourneyKey] = useState('');
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

  const saveJourney = useCallback(async (statusOverride = null) => {
    setBusy(true);
    setStatusText('Journey kaydediliyor...');

    try {
      const finalStatus = statusOverride || journeyStatus;
      for (const node of nodes) {
        const data = normalizeNodeData(node.data, node.id);
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
      {viewMode === 'designer' && (
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
            onClick={() => setShowInspector((prev) => !prev)}
            disabled={busy}
          >
            {showInspector ? 'Hide Inspector' : 'Inspector'}
          </button>
          <button
            type="button"
            onClick={() => setShowJourneyLogs((prev) => !prev)}
            disabled={busy}
          >
            {showJourneyLogs ? 'Hide Logs' : 'Show Logs'}
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
            onClick={fetchJourneyLogs}
            disabled={busy || journeyLogsLoading}
          >
            {journeyLogsLoading ? 'Loading Logs...' : 'Load Last 1h Logs'}
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
                  <input
                    value={selectedData.event_type}
                    onChange={(e) => updateSelectedNode({ event_type: e.target.value })}
                  />
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
                    http_method
                    <select
                      value={selectedData.http_method}
                      onChange={(e) => updateSelectedNode({ http_method: e.target.value })}
                    >
                      <option value="GET">GET</option>
                      <option value="POST">POST</option>
                      <option value="PUT">PUT</option>
                      <option value="PATCH">PATCH</option>
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
                      <input
                        value={selectedData.condition_event_type}
                        onChange={(e) =>
                          updateSelectedNode({ condition_event_type: e.target.value })
                        }
                      />
                    </label>
                  )}
                  {selectedData.condition_key === 'segment_match' && (
                    <label>
                      condition_segment_value
                      <input
                        value={selectedData.condition_segment_value}
                        onChange={(e) =>
                          updateSelectedNode({ condition_segment_value: e.target.value })
                        }
                      />
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
                    <input
                      value={selectedData.template_id}
                      onChange={(e) => updateSelectedNode({ template_id: e.target.value })}
                    />
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

      {viewMode === 'library' && (
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
      {viewMode !== 'library' && <div className="statusBar">{statusText}</div>}
      </div>
    </div>
  );
}

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
