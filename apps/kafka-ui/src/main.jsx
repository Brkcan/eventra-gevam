import React, { useCallback, useEffect, useMemo, useState } from 'react';
import ReactDOM from 'react-dom/client';
import './styles.css';

function resolveListenerHubBaseUrl() {
  const configured = String(import.meta.env.VITE_LISTENER_HUB_BASE_URL || '').trim();
  if (typeof window === 'undefined') {
    return configured || 'http://127.0.0.1:3020';
  }

  const { protocol, hostname } = window.location;
  const isLocalHost = hostname === 'localhost' || hostname === '127.0.0.1';
  if (configured) {
    return configured;
  }
  if (isLocalHost) {
    return 'http://127.0.0.1:3020';
  }
  return `${protocol}//${hostname}:3020`;
}

const LISTENER_HUB_BASE_URL = resolveListenerHubBaseUrl();
const DEFAULT_FORM = {
  original_listener_id: '',
  listener_id: '',
  name: '',
  enabled: true,
  brokers_csv: '',
  topic: '',
  group_id: '',
  event_names_csv: '',
  mapping_json:
    '{\n  "event_id": "$.meta.id",\n  "customer_id": "$.user.customer_id",\n  "event_type": "$.event.name",\n  "ts": "$.event.ts",\n  "source": "mobile-app"\n}',
  output_topic: 'event.raw',
  output_key_field: 'customer_id',
  retry_max_retries: 5,
  retry_backoff_ms: 1000,
  log_raw_payload: false,
  log_normalized_event: false
};

const DEFAULT_SAMPLE_PAYLOAD = '{\n  "meta": {\n    "id": "sample-1"\n  },\n  "user": {\n    "customer_id": "cust-1"\n  },\n  "event": {\n    "name": "app_open",\n    "ts": "2026-04-04T12:00:00.000Z"\n  }\n}';

function parseCsvList(raw) {
  return String(raw || '')
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
}

function parseJsonText(raw) {
  try {
    return { ok: true, value: JSON.parse(String(raw || '{}')) };
  } catch (error) {
    return { ok: false, message: error.message };
  }
}

function parseMappingRows(raw) {
  const parsed = parseJsonText(raw);
  if (!parsed.ok || !parsed.value || typeof parsed.value !== 'object' || Array.isArray(parsed.value)) {
    return [];
  }
  return Object.entries(parsed.value).map(([target, source], index) => ({
    id: `row-${index}-${target}`,
    target,
    source: String(source || '')
  }));
}

function buildMappingObject(rows) {
  const result = {};
  for (const row of rows) {
    const target = String(row.target || '').trim();
    const source = String(row.source || '').trim();
    if (!target || !source) continue;
    result[target] = source;
  }
  return result;
}

function JsonTreeNode({ name, value, depth = 0 }) {
  const indent = { paddingLeft: `${depth * 16}px` };
  const isObject = value && typeof value === 'object';

  if (!isObject) {
    return (
      <div className="jsonTreeRow" style={indent}>
        {name ? <span className="jsonTreeKey">{name}:</span> : null}
        <span className="jsonTreeValue">{String(value)}</span>
      </div>
    );
  }

  const entries = Array.isArray(value)
    ? value.map((item, index) => [String(index), item])
    : Object.entries(value);

  return (
    <div className="jsonTreeBlock">
      {name ? (
        <div className="jsonTreeRow" style={indent}>
          <span className="jsonTreeKey">{name}</span>
        </div>
      ) : null}
      {entries.length === 0 ? (
        <div className="jsonTreeRow" style={{ paddingLeft: `${(depth + 1) * 16}px` }}>
          <span className="jsonTreeValue">{Array.isArray(value) ? '[]' : '{}'}</span>
        </div>
      ) : (
        entries.map(([entryKey, entryValue]) => (
          <JsonTreeNode key={`${name || 'root'}-${entryKey}`} name={entryKey} value={entryValue} depth={depth + 1} />
        ))
      )}
    </div>
  );
}

function App() {
  const [loading, setLoading] = useState(false);
  const [statusText, setStatusText] = useState('Hazir');
  const [configs, setConfigs] = useState([]);
  const [runtimeItems, setRuntimeItems] = useState([]);
  const [form, setForm] = useState(DEFAULT_FORM);
  const [activeView, setActiveView] = useState('editor');
  const [searchText, setSearchText] = useState('');
  const [editorSection, setEditorSection] = useState('source');
  const [mappingRows, setMappingRows] = useState(() => parseMappingRows(DEFAULT_FORM.mapping_json));
  const [samplePayloadText, setSamplePayloadText] = useState(DEFAULT_SAMPLE_PAYLOAD);
  const [previewResult, setPreviewResult] = useState(null);
  const [sampleConsumeResult, setSampleConsumeResult] = useState(null);

  const applyConfigToForm = useCallback((item) => {
    const nextMappingJson = JSON.stringify(item?.mapping?.fields || {}, null, 2);
    setForm({
      original_listener_id: item.listener_id || '',
      listener_id: item.listener_id || '',
      name: item.name || '',
      enabled: Boolean(item.enabled),
      brokers_csv: Array.isArray(item?.source?.brokers) ? item.source.brokers.join(', ') : '',
      topic: item?.source?.topic || '',
      group_id: item?.source?.group_id || '',
      event_names_csv: Array.isArray(item?.filter?.event_names) ? item.filter.event_names.join(', ') : '',
      mapping_json: nextMappingJson,
      output_topic: item?.output?.topic || 'event.raw',
      output_key_field: item?.output?.key_field || 'customer_id',
      retry_max_retries: Number(item?.retry?.max_retries || 5),
      retry_backoff_ms: Number(item?.retry?.backoff_ms || 1000),
      log_raw_payload: Boolean(item?.debug?.log_raw_payload),
      log_normalized_event: Boolean(item?.debug?.log_normalized_event)
    });
    setMappingRows(parseMappingRows(nextMappingJson));
  }, []);

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const [configsRes, runtimeRes] = await Promise.all([
        fetch(`${LISTENER_HUB_BASE_URL}/admin/configs`),
        fetch(`${LISTENER_HUB_BASE_URL}/listeners`)
      ]);
      if (!configsRes.ok) {
        throw new Error(`Listener configs failed: ${configsRes.status}`);
      }
      if (!runtimeRes.ok) {
        throw new Error(`Listener runtime failed: ${runtimeRes.status}`);
      }
      const [configsBody, runtimeBody] = await Promise.all([configsRes.json(), runtimeRes.json()]);
      const configItems = Array.isArray(configsBody.items) ? configsBody.items : [];
      setConfigs(configItems);
      setRuntimeItems(Array.isArray(runtimeBody.items) ? runtimeBody.items : []);
      if (!form.original_listener_id && configItems.length > 0) {
        applyConfigToForm(configItems[0]);
      }
      setStatusText(`Listener listesi yuklendi: ${configItems.length}`);
    } catch (error) {
      setStatusText(`Yukleme hatasi: ${error.message}`);
    } finally {
      setLoading(false);
    }
  }, [applyConfigToForm, form.original_listener_id]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  const runtimeMap = useMemo(
    () => new Map(runtimeItems.map((item) => [item.listener_id, item])),
    [runtimeItems]
  );
  const filteredConfigs = useMemo(() => {
    const query = String(searchText || '').trim().toLowerCase();
    if (!query) {
      return configs;
    }
    return configs.filter((item) => {
      const haystack = [item.listener_id, item.name, item?.source?.topic]
        .map((value) => String(value || '').toLowerCase())
        .join(' ');
      return haystack.includes(query);
    });
  }, [configs, searchText]);

  const addMappingRow = useCallback(() => {
    setMappingRows((current) => [...current, { id: `row-${Date.now()}`, target: '', source: '' }]);
  }, []);

  const updateMappingRow = useCallback((rowId, field, value) => {
    setMappingRows((current) =>
      current.map((row) => (row.id === rowId ? { ...row, [field]: value } : row))
    );
  }, []);

  const deleteMappingRow = useCallback((rowId) => {
    setMappingRows((current) => current.filter((row) => row.id !== rowId));
  }, []);

  const buildPayload = useCallback(() => {
    const mappingObject = buildMappingObject(mappingRows);
    if (Object.keys(mappingObject).length === 0) {
      throw new Error('En az bir mapping satiri gerekli.');
    }
    return {
      listener_id: String(form.listener_id || '').trim(),
      name: String(form.name || '').trim(),
      enabled: Boolean(form.enabled),
      source_type: 'kafka_topic',
      source: {
        ...(parseCsvList(form.brokers_csv || '').length
          ? { brokers: parseCsvList(form.brokers_csv || '') }
          : {}),
        topic: String(form.topic || '').trim(),
        group_id: String(form.group_id || '').trim()
      },
      filter: {
        event_names: parseCsvList(form.event_names_csv || '')
      },
      debug: {
        log_raw_payload: Boolean(form.log_raw_payload),
        log_normalized_event: Boolean(form.log_normalized_event)
      },
      mapping: {
        mode: 'declarative',
        fields: mappingObject
      },
      output: {
        topic: String(form.output_topic || 'event.raw').trim(),
        key_field: String(form.output_key_field || 'customer_id').trim()
      },
      retry: {
        max_retries: Math.max(1, Number(form.retry_max_retries) || 1),
        backoff_ms: Math.max(1, Number(form.retry_backoff_ms) || 1)
      }
    };
  }, [form, mappingRows]);

  const buildPreviewPayload = useCallback(() => {
    const payload = buildPayload();
    return {
      ...payload,
      source_type: 'kafka_topic'
    };
  }, [buildPayload]);

  const saveConfig = useCallback(async () => {
    try {
      setLoading(true);
      const payload = buildPayload();
      if (!payload.listener_id || !payload.name || !payload.source.topic || !payload.source.group_id) {
        throw new Error('listener_id, name, topic ve group_id zorunlu.');
      }
      const isEdit = Boolean(form.original_listener_id);
      const url = isEdit
        ? `${LISTENER_HUB_BASE_URL}/admin/configs/${encodeURIComponent(form.original_listener_id)}`
        : `${LISTENER_HUB_BASE_URL}/admin/configs`;
      const response = await fetch(url, {
        method: isEdit ? 'PUT' : 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(payload)
      });
      const body = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(body.message || `Listener config save failed: ${response.status}`);
      }
      applyConfigToForm(body.item || payload);
      await fetchData();
      setStatusText(`Listener kaydedildi: ${payload.listener_id}`);
    } catch (error) {
      setStatusText(`Kayit hatasi: ${error.message}`);
    } finally {
      setLoading(false);
    }
  }, [applyConfigToForm, buildPayload, fetchData, form.original_listener_id]);

  const deleteConfig = useCallback(async () => {
    const targetId = String(form.original_listener_id || form.listener_id || '').trim();
    if (!targetId) {
      setStatusText('Silmek icin listener sec.');
      return;
    }
    try {
      setLoading(true);
      const response = await fetch(`${LISTENER_HUB_BASE_URL}/admin/configs/${encodeURIComponent(targetId)}`, {
        method: 'DELETE'
      });
      const body = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(body.message || `Listener config delete failed: ${response.status}`);
      }
      setForm(DEFAULT_FORM);
      await fetchData();
      setStatusText(`Listener silindi: ${targetId}`);
    } catch (error) {
      setStatusText(`Silme hatasi: ${error.message}`);
    } finally {
      setLoading(false);
    }
  }, [fetchData, form.listener_id, form.original_listener_id]);

  const reloadConfigs = useCallback(async () => {
    try {
      setLoading(true);
      const response = await fetch(`${LISTENER_HUB_BASE_URL}/admin/reload`, { method: 'POST' });
      const body = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(body.message || `Listener reload failed: ${response.status}`);
      }
      await fetchData();
      setStatusText(`Reload tamamlandi: ${body.listeners_total || 0} listener`);
    } catch (error) {
      setStatusText(`Reload hatasi: ${error.message}`);
    } finally {
      setLoading(false);
    }
  }, [fetchData]);

  const previewNormalize = useCallback(async () => {
    try {
      setLoading(true);
      const parsedSample = parseJsonText(samplePayloadText || '{}');
      if (!parsedSample.ok) {
        throw new Error(`sample payload gecersiz: ${parsedSample.message}`);
      }
      const response = await fetch(`${LISTENER_HUB_BASE_URL}/admin/preview/normalize`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          listener: buildPreviewPayload(),
          payload: parsedSample.value
        })
      });
      const body = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(body.message || `Preview normalize failed: ${response.status}`);
      }
      setPreviewResult(body.item || null);
      setStatusText('Normalize preview hazirlandi.');
    } catch (error) {
      setStatusText(`Normalize preview hatasi: ${error.message}`);
    } finally {
      setLoading(false);
    }
  }, [buildPreviewPayload, samplePayloadText]);

  const consumeSample = useCallback(async () => {
    try {
      setLoading(true);
      const response = await fetch(`${LISTENER_HUB_BASE_URL}/admin/preview/consume-sample`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          listener: buildPreviewPayload(),
          timeout_ms: 8000
        })
      });
      const body = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(body.message || `Sample consume failed: ${response.status}`);
      }
      setSampleConsumeResult(body.item || null);
      if (body.item?.raw_payload) {
        setSamplePayloadText(JSON.stringify(body.item.raw_payload, null, 2));
      }
      if (body.item?.normalized_event) {
        setPreviewResult({
          matches_filter: body.item.matches_filter,
          normalized_event: body.item.normalized_event
        });
      }
      setStatusText('Topic uzerinden sample payload alindi.');
    } catch (error) {
      setStatusText(`Sample consume hatasi: ${error.message}`);
    } finally {
      setLoading(false);
    }
  }, [buildPreviewPayload]);

  return (
    <div className="kafkaUiShell">
      <aside className="kafkaSidebar">
        <div className="kafkaBrand">EVENTRA KAFKA UI</div>
        <button type="button" className="kafkaPrimaryButton" onClick={() => setForm(DEFAULT_FORM)}>
          Yeni Listener
        </button>
        <div className="kafkaMenu">
          <button
            type="button"
            className={`kafkaMenuItem ${activeView === 'editor' ? 'active' : ''}`}
            onClick={() => setActiveView('editor')}
          >
            Editor
          </button>
          <button
            type="button"
            className={`kafkaMenuItem ${activeView === 'preview' ? 'active' : ''}`}
            onClick={() => setActiveView('preview')}
          >
            Preview
          </button>
          <button
            type="button"
            className={`kafkaMenuItem ${activeView === 'runtime' ? 'active' : ''}`}
            onClick={() => setActiveView('runtime')}
          >
            Runtime
          </button>
        </div>
        <input
          className="kafkaSearchInput"
          value={searchText}
          onChange={(e) => setSearchText(e.target.value)}
          placeholder="Listener ara..."
        />
        <div className="kafkaList">
          {filteredConfigs.map((item) => {
            const runtime = runtimeMap.get(item.listener_id);
            return (
              <button
                type="button"
                key={item.listener_id}
                className={`kafkaListItem ${form.listener_id === item.listener_id ? 'active' : ''}`}
                onClick={() => applyConfigToForm(item)}
              >
                <div className="kafkaListItemHead">
                  <strong>{item.name}</strong>
                  <span
                    className={`kafkaStatusBadge small status-${
                      runtime?.status || (item.enabled ? 'configured' : 'disabled')
                    }`}
                  >
                    {runtime?.status || (item.enabled ? 'configured' : 'disabled')}
                  </span>
                </div>
                <span>{item.listener_id}</span>
                <small>{item?.source?.topic || '-'}</small>
              </button>
            );
          })}
          {filteredConfigs.length === 0 && <div className="kafkaEmptyState">Sonuc bulunamadi.</div>}
        </div>
      </aside>

      <main className="kafkaPage">
        <header className="kafkaHeader">
          <div>
            <h1>Kafka Listener Studio</h1>
            <p>Listener tanimla, payload mapping kur ve runtime durumunu ayri bir uygulamada yonet.</p>
          </div>
          <div className="kafkaHeaderActions">
            <button type="button" onClick={fetchData} disabled={loading}>
              {loading ? 'Yukleniyor...' : 'Yenile'}
            </button>
            <button type="button" onClick={reloadConfigs} disabled={loading}>
              Reload
            </button>
          </div>
        </header>

        <section className="kafkaStatusRow">
          <div className="kafkaStatusCard">
            <span>Listener</span>
            <strong>{configs.length}</strong>
          </div>
          <div className="kafkaStatusCard">
            <span>Running</span>
            <strong>{runtimeItems.filter((item) => item.status === 'running').length}</strong>
          </div>
          <div className="kafkaStatusCard">
            <span>Error</span>
            <strong>{runtimeItems.filter((item) => item.status === 'error').length}</strong>
          </div>
        </section>

        <section className="kafkaWorkspace">
          {activeView === 'editor' && (
            <section className="kafkaPanel kafkaPanelSingle">
              <div className="kafkaPanelHead">
                <h2>Config Editor</h2>
                <small>Dosya tabanli listener tanimi</small>
              </div>
              <div className="kafkaAccordionList">
                <section className={`kafkaAccordionItem ${editorSection === 'source' ? 'active' : ''}`}>
                  <button type="button" className="kafkaAccordionTrigger" onClick={() => setEditorSection(editorSection === 'source' ? '' : 'source')}>
                    <span>Source</span>
                    <strong>{editorSection === 'source' ? '−' : '+'}</strong>
                  </button>
                  {editorSection === 'source' && (
                    <div className="kafkaFormGrid">
                      <label>
                        Listener Id
                        <input value={form.listener_id} onChange={(e) => setForm((current) => ({ ...current, listener_id: e.target.value }))} />
                      </label>
                      <label>
                        Name
                        <input value={form.name} onChange={(e) => setForm((current) => ({ ...current, name: e.target.value }))} />
                      </label>
                      <label>
                        Brokers
                        <input value={form.brokers_csv} onChange={(e) => setForm((current) => ({ ...current, brokers_csv: e.target.value }))} placeholder="bos birakirsan global broker kullanilir" />
                      </label>
                      <label>
                        Topic
                        <input value={form.topic} onChange={(e) => setForm((current) => ({ ...current, topic: e.target.value }))} />
                      </label>
                      <label>
                        Group Id
                        <input value={form.group_id} onChange={(e) => setForm((current) => ({ ...current, group_id: e.target.value }))} />
                      </label>
                      <label>
                        Event Names
                        <input value={form.event_names_csv} onChange={(e) => setForm((current) => ({ ...current, event_names_csv: e.target.value }))} placeholder="order_created, order_updated" />
                      </label>
                      <label className="kafkaCheck">
                        <input type="checkbox" checked={form.enabled} onChange={(e) => setForm((current) => ({ ...current, enabled: e.target.checked }))} />
                        Enabled
                      </label>
                    </div>
                  )}
                </section>

                <section className={`kafkaAccordionItem ${editorSection === 'mapping' ? 'active' : ''}`}>
                  <button type="button" className="kafkaAccordionTrigger" onClick={() => setEditorSection(editorSection === 'mapping' ? '' : 'mapping')}>
                    <span>Mapping</span>
                    <strong>{editorSection === 'mapping' ? '−' : '+'}</strong>
                  </button>
                  {editorSection === 'mapping' && (
                    <div className="kafkaFormGrid">
                      <div className="kafkaTextarea">
                        <div className="mappingBuilderHead">
                          <strong>Mapping Rows</strong>
                          <button type="button" onClick={addMappingRow}>
                            Alan Ekle
                          </button>
                        </div>
                        <div className="mappingBuilderTable">
                          <div className="mappingBuilderRow head">
                            <span>Target Field</span>
                            <span>Source Path / Literal</span>
                            <span>Sil</span>
                          </div>
                          {mappingRows.map((row) => (
                            <div key={row.id} className="mappingBuilderRow">
                              <input
                                value={row.target}
                                onChange={(e) => updateMappingRow(row.id, 'target', e.target.value)}
                                placeholder="payload.screen"
                              />
                              <input
                                value={row.source}
                                onChange={(e) => updateMappingRow(row.id, 'source', e.target.value)}
                                placeholder="$.context.screen"
                              />
                              <button
                                type="button"
                                className="mappingDeleteButton"
                                onClick={() => deleteMappingRow(row.id)}
                              >
                                X
                              </button>
                            </div>
                          ))}
                          {mappingRows.length === 0 && (
                            <div className="mappingBuilderEmpty">Mapping satiri yok.</div>
                          )}
                        </div>
                      </div>
                      <label className="kafkaCheck">
                        <input type="checkbox" checked={form.log_raw_payload} onChange={(e) => setForm((current) => ({ ...current, log_raw_payload: e.target.checked }))} />
                        Raw payload logla
                      </label>
                      <label className="kafkaCheck">
                        <input type="checkbox" checked={form.log_normalized_event} onChange={(e) => setForm((current) => ({ ...current, log_normalized_event: e.target.checked }))} />
                        Normalized event logla
                      </label>
                    </div>
                  )}
                </section>

                <section className={`kafkaAccordionItem ${editorSection === 'delivery' ? 'active' : ''}`}>
                  <button type="button" className="kafkaAccordionTrigger" onClick={() => setEditorSection(editorSection === 'delivery' ? '' : 'delivery')}>
                    <span>Delivery</span>
                    <strong>{editorSection === 'delivery' ? '−' : '+'}</strong>
                  </button>
                  {editorSection === 'delivery' && (
                    <div className="kafkaFormGrid">
                      <label>
                        Output Topic
                        <input value={form.output_topic} onChange={(e) => setForm((current) => ({ ...current, output_topic: e.target.value }))} />
                      </label>
                      <label>
                        Output Key Field
                        <input value={form.output_key_field} onChange={(e) => setForm((current) => ({ ...current, output_key_field: e.target.value }))} />
                      </label>
                      <label>
                        Max Retries
                        <input type="number" min="1" value={form.retry_max_retries} onChange={(e) => setForm((current) => ({ ...current, retry_max_retries: Number(e.target.value || 1) }))} />
                      </label>
                      <label>
                        Backoff Ms
                        <input type="number" min="1" value={form.retry_backoff_ms} onChange={(e) => setForm((current) => ({ ...current, retry_backoff_ms: Number(e.target.value || 1000) }))} />
                      </label>
                    </div>
                  )}
                </section>
              </div>
              <div className="kafkaPanelActions">
                <button type="button" className="primary" onClick={saveConfig} disabled={loading}>
                  Kaydet
                </button>
                <button type="button" className="danger" onClick={deleteConfig} disabled={loading}>
                  Sil
                </button>
              </div>
            </section>
          )}

          {activeView === 'preview' && (
            <section className="kafkaPanel kafkaPanelSingle">
              <div className="kafkaPanelHead">
                <h2>Payload Preview</h2>
                <small>Ornek payload yapistir veya topic'ten tek sample cek</small>
              </div>
              <div className="kafkaFormGrid kafkaPreviewGrid">
                <label className="kafkaTextarea">
                  Sample Payload JSON
                  <textarea value={samplePayloadText} onChange={(e) => setSamplePayloadText(e.target.value)} />
                </label>
              </div>
              <div className="kafkaPanelActions">
                <button type="button" onClick={previewNormalize} disabled={loading}>
                  Normalize Preview
                </button>
                <button type="button" onClick={consumeSample} disabled={loading}>
                  Topic'ten Sample Cek
                </button>
              </div>
              <div className="kafkaPreviewResults">
                <div>
                  <h3>Preview Sonucu</h3>
                  <div className="jsonTreePanel">
                    <JsonTreeNode value={previewResult || { matches_filter: null, normalized_event: null }} />
                  </div>
                </div>
                <div>
                  <h3>Consume Sonucu</h3>
                  <div className="jsonTreePanel">
                    <JsonTreeNode value={sampleConsumeResult || { raw_payload: null }} />
                  </div>
                </div>
              </div>
            </section>
          )}

          {activeView === 'runtime' && (
            <section className="kafkaPanel kafkaPanelSingle">
              <div className="kafkaPanelHead">
                <h2>Runtime</h2>
                <small>Listener durumlari ve son hata</small>
              </div>
              <table className="kafkaTable">
                <thead>
                  <tr>
                    <th>Listener</th>
                    <th>Topic</th>
                    <th>Status</th>
                    <th>Last Error</th>
                  </tr>
                </thead>
                <tbody>
                  {runtimeItems.map((item) => (
                    <tr key={item.listener_id}>
                      <td>{item.name}</td>
                      <td>{item.source_topic || '-'}</td>
                      <td>
                        <span className={`kafkaStatusBadge status-${item.status}`}>{item.status}</span>
                      </td>
                      <td>{item.last_error || '-'}</td>
                    </tr>
                  ))}
                  {runtimeItems.length === 0 && (
                    <tr>
                      <td colSpan={4}>Runtime verisi yok.</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </section>
          )}
        </section>

        <footer className="kafkaFooter">{statusText}</footer>
      </main>
    </div>
  );
}

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
