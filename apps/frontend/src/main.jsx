import React, { useCallback, useEffect, useMemo, useState } from 'react';
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

  return {
    ...edge,
    label:
      inferredType === 'always' &&
      delayMinutes === 0 &&
      priority === 100 &&
      !expression &&
      rateLimitPerDay === 0
        ? undefined
        : `${inferredType} | p:${priority} | d:${delayMinutes}m | rl:${rateLimitPerDay}/d`,
    data: {
      ...currentData,
      condition_result: inferredType === 'true' || inferredType === 'false' ? inferredType : '',
      edge_type: inferredType,
      priority,
      delay_minutes: delayMinutes,
      expression,
      rate_limit_per_day: rateLimitPerDay
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
  const [reactFlowInstance, setReactFlowInstance] = useState(null);
  const [journeyItems, setJourneyItems] = useState([]);
  const [selectedJourneyKey, setSelectedJourneyKey] = useState('');
  const [journeyListQuery, setJourneyListQuery] = useState('');
  const [journeyListStatus, setJourneyListStatus] = useState('all');
  const [journeyListLimit, setJourneyListLimit] = useState(25);
  const [journeyListOffset, setJourneyListOffset] = useState(0);
  const [journeyListMeta, setJourneyListMeta] = useState({
    total: 0,
    has_more: false,
    limit: 25,
    offset: 0
  });

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
  }, []);

  const onEdgeClick = useCallback((_event, edge) => {
    setSelectedEdgeId(edge.id);
    setSelectedNodeId(null);
  }, []);

  const onPaneClick = useCallback(() => {
    setSelectedNodeId(null);
    setSelectedEdgeId(null);
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

  const onDragStart = useCallback((event, nodeKind) => {
    event.dataTransfer.setData('application/eventra-node-kind', nodeKind);
    event.dataTransfer.effectAllowed = 'move';
  }, []);

  const onDragOverCanvas = useCallback((event) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = 'move';
  }, []);

  const onDropCanvas = useCallback(
    (event) => {
      event.preventDefault();
      const nodeKind = event.dataTransfer.getData('application/eventra-node-kind');
      if (!nodeKind || !reactFlowInstance) {
        return;
      }

      const position = reactFlowInstance.screenToFlowPosition({
        x: event.clientX,
        y: event.clientY
      });

      addNode(nodeKind, position);
    },
    [addNode, reactFlowInstance]
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
      setStatusText(`Yuklendi: ${journey.journey_id} v${journey.version}`);
    },
    [setEdges, setNodes]
  );

  const fetchJourneys = useCallback(async (overrides = {}) => {
    const shouldStore = overrides.store !== false;
    const finalQuery = overrides.query ?? journeyListQuery;
    const finalStatus = overrides.status ?? journeyListStatus;
    const finalLimit = overrides.limit ?? journeyListLimit;
    const finalOffset = overrides.offset ?? journeyListOffset;
    const finalJourneyId = overrides.journey_id ?? null;
    const finalSortBy = overrides.sort_by || 'updated_at';
    const finalSortOrder = overrides.sort_order || 'desc';

    const params = new URLSearchParams();
    params.set('limit', String(finalLimit));
    params.set('offset', String(finalOffset));
    params.set('sort_by', finalSortBy);
    params.set('sort_order', finalSortOrder);
    if (finalQuery) {
      params.set('journey_id', finalQuery);
    }
    if (finalJourneyId) {
      params.set('journey_id', finalJourneyId);
    }
    if (finalStatus && finalStatus !== 'all') {
      params.set('status', finalStatus);
    }

    const response = await fetch(`${API_BASE_URL}/journeys?${params.toString()}`);
    if (!response.ok) {
      throw new Error(`Journey list failed: ${response.status}`);
    }
    const body = await response.json();
    const items = body.items || [];
    const meta = body.meta || {};
    if (shouldStore) {
      setJourneyItems(items);
      setJourneyListMeta({
        total: Number(meta.total || 0),
        has_more: Boolean(meta.has_more),
        limit: Number(meta.limit || finalLimit),
        offset: Number(meta.offset || finalOffset)
      });
    }
    return items;
  }, [journeyListLimit, journeyListOffset, journeyListQuery, journeyListStatus]);

  const loadJourney = useCallback(async () => {
    setBusy(true);
    setStatusText('Journey yukleniyor...');

    try {
      const items = await fetchJourneys({
        journey_id: journeyId,
        limit: 100,
        offset: 0,
        sort_by: 'version',
        sort_order: 'desc',
        status: 'all',
        store: false
      });
      const latest = getLatestJourney(items, journeyId);
      if (!latest) {
        setStatusText('Journey bulunamadi, varsayilan akis gosteriliyor');
        return;
      }
      applyJourneyToCanvas(latest);
    } catch (error) {
      setStatusText(`Yukleme hatasi: ${error.message}`);
    } finally {
      setBusy(false);
    }
  }, [applyJourneyToCanvas, fetchJourneys, journeyId]);

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

      const listResponse = await fetch(`${API_BASE_URL}/journeys`);
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
      setSelectedJourneyKey(`${journeyId}::${version}`);
    } catch (error) {
      setStatusText(`Kayit hatasi: ${error.message}`);
    } finally {
      setBusy(false);
    }
  }, [fetchJourneys, graphJson, journeyId, journeyStatus, name, nodes, version]);

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
    setStatusText('Yeni journey olusturuldu. Canvas bos, node ekleyerek baslayabilirsin.');
  }, [setEdges, setNodes]);

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
      setSelectedJourneyKey('');
      newJourney();
      setStatusText(`${journeyId} v${version} silindi.`);
    } catch (error) {
      setStatusText(`Journey silme hatasi: ${error.message}`);
    } finally {
      setBusy(false);
    }
  }, [fetchJourneys, journeyId, newJourney, version]);

  useEffect(() => {
    fetchJourneys().catch((error) => {
      setStatusText(`Journey listesi yuklenemedi: ${error.message}`);
    });
  }, [fetchJourneys]);

  useEffect(() => {
    setJourneyListOffset(0);
  }, [journeyListQuery, journeyListStatus, journeyListLimit]);

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
    <div className="page">
      <header className="topbar">
        <h1>Eventra Journey Designer</h1>
        <span>MVP v0.3</span>
      </header>

      <section className="controlsBar">
        <label>
          Journey List
          <select
            value={selectedJourneyKey}
            onChange={(e) => {
              const value = e.target.value;
              setSelectedJourneyKey(value);
              const selected = journeyItems.find(
                (item) => `${item.journey_id}::${item.version}` === value
              );
              if (selected) {
                applyJourneyToCanvas(selected);
              }
            }}
          >
            <option value="">Select journey...</option>
            {journeyItems.map((item) => (
              <option
                key={`${item.journey_id}::${item.version}`}
                value={`${item.journey_id}::${item.version}`}
              >
                {item.journey_id} v{item.version} [{item.status}]
              </option>
            ))}
          </select>
        </label>
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
        <div className="actions">
          <button
            type="button"
            onClick={async () => {
              try {
                setBusy(true);
                await fetchJourneys();
                setStatusText('Journey listesi yenilendi.');
              } catch (error) {
                setStatusText(`Liste yenileme hatasi: ${error.message}`);
              } finally {
                setBusy(false);
              }
            }}
            disabled={busy}
          >
            Refresh List
          </button>
          <button type="button" onClick={newJourney} disabled={busy}>
            New Journey
          </button>
          <button
            type="button"
            onClick={() => saveJourney('draft')}
            disabled={busy}
          >
            Save Draft
          </button>
          <button
            type="button"
            onClick={rollbackSelectedVersion}
            disabled={busy || !selectedJourneyItem}
          >
            Rollback Selected
          </button>
          <button type="button" className="danger" onClick={deleteJourney} disabled={busy}>
            Delete Journey
          </button>
          <button type="button" onClick={loadJourney} disabled={busy}>
            Load
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

      <section className="listBar">
        <label>
          List Search
          <input
            placeholder="journey_id ara..."
            value={journeyListQuery}
            onChange={(e) => setJourneyListQuery(e.target.value)}
          />
        </label>
        <label>
          List Status
          <select
            value={journeyListStatus}
            onChange={(e) => setJourneyListStatus(e.target.value)}
          >
            <option value="all">all</option>
            <option value="draft">draft</option>
            <option value="published">published</option>
            <option value="archived">archived</option>
          </select>
        </label>
        <label>
          Page Size
          <select
            value={journeyListLimit}
            onChange={(e) => setJourneyListLimit(Number(e.target.value) || 25)}
          >
            <option value="10">10</option>
            <option value="25">25</option>
            <option value="50">50</option>
            <option value="100">100</option>
          </select>
        </label>
        <div className="pager">
          <button
            type="button"
            disabled={busy || journeyListOffset <= 0}
            onClick={() =>
              setJourneyListOffset((prev) => Math.max(0, prev - journeyListLimit))
            }
          >
            Prev
          </button>
          <span>
            {journeyListMeta.total === 0
              ? '0 / 0'
              : `${journeyListMeta.offset + 1}-${Math.min(
                  journeyListMeta.offset + journeyListMeta.limit,
                  journeyListMeta.total
                )} / ${journeyListMeta.total}`}
          </span>
          <button
            type="button"
            disabled={busy || !journeyListMeta.has_more}
            onClick={() =>
              setJourneyListOffset((prev) => prev + journeyListLimit)
            }
          >
            Next
          </button>
        </div>
      </section>

      <section className="builderBar">
        <span>Add Node:</span>
        <button type="button" onClick={() => addNode('trigger')}>+ Trigger</button>
        <button type="button" onClick={() => addNode('wait')}>+ Wait</button>
        <button type="button" onClick={() => addNode('http_call')}>+ HTTP Call</button>
        <button type="button" onClick={() => addNode('condition')}>+ Condition</button>
        <button type="button" onClick={() => addNode('action')}>+ Action</button>
        <span className="dragHint">Drag To Canvas:</span>
        <button type="button" draggable onDragStart={(e) => onDragStart(e, 'trigger')}>
          Drag Trigger
        </button>
        <button type="button" draggable onDragStart={(e) => onDragStart(e, 'wait')}>
          Drag Wait
        </button>
        <button type="button" draggable onDragStart={(e) => onDragStart(e, 'http_call')}>
          Drag HTTP
        </button>
        <button type="button" draggable onDragStart={(e) => onDragStart(e, 'condition')}>
          Drag Condition
        </button>
        <button type="button" draggable onDragStart={(e) => onDragStart(e, 'action')}>
          Drag Action
        </button>
        <button
          type="button"
          onClick={deleteSelected}
          disabled={!selectedNodeId && !selectedEdgeId}
        >
          Delete Selected
        </button>
      </section>

      <div className="statusBar">{statusText}</div>

      <main className="workspace">
        <section className="canvas">
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onConnect={onConnect}
            onNodeClick={onNodeClick}
            onEdgeClick={onEdgeClick}
            onPaneClick={onPaneClick}
            onDragOver={onDragOverCanvas}
            onDrop={onDropCanvas}
            onInit={setReactFlowInstance}
            fitView
          >
            <MiniMap />
            <Controls />
            <Background gap={20} />
          </ReactFlow>
        </section>

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
            </div>
          )}
        </aside>
      </main>
    </div>
  );
}

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
