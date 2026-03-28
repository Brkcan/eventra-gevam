import crypto from 'node:crypto';
import OpenAI from 'openai';
import { z } from 'zod';

const NodeKindSchema = z.enum(['trigger', 'wait', 'condition', 'action', 'http_call', 'cache_lookup']);

const GraphNodeSchema = z.object({
  id: z.string().min(1),
  position: z
    .object({
      x: z.number(),
      y: z.number()
    })
    .optional(),
  data: z
    .object({
      label: z.string().optional(),
      node_kind: NodeKindSchema,
      event_type: z.string().optional(),
      wait_minutes: z.number().optional(),
      manual_release: z.boolean().optional(),
      cache_dataset_key: z.string().optional(),
      cache_lookup_key_template: z.string().optional(),
      cache_target_path: z.string().optional(),
      cache_on_miss: z.string().optional(),
      cache_default_json: z.string().optional(),
      http_method: z.string().optional(),
      endpoint_id: z.string().optional(),
      http_url: z.string().optional(),
      http_headers_json: z.string().optional(),
      http_body_template: z.string().optional(),
      http_timeout_ms: z.number().optional(),
      response_mapping_json: z.string().optional(),
      condition_key: z.string().optional(),
      condition_event_type: z.string().optional(),
      condition_segment_value: z.string().optional(),
      channel: z.string().optional(),
      template_id: z.string().optional()
    })
    .passthrough()
});

const GraphEdgeSchema = z.object({
  id: z.string().min(1),
  source: z.string().min(1),
  target: z.string().min(1),
  animated: z.boolean().optional(),
  data: z
    .object({
      condition_result: z.string().optional(),
      edge_type: z.string().optional(),
      priority: z.number().optional(),
      delay_minutes: z.number().optional(),
      expression: z.string().optional(),
      rate_limit_per_day: z.number().optional(),
      max_customers_total: z.number().optional(),
      max_customers_per_day: z.number().optional()
    })
    .passthrough()
    .optional()
});

const JourneyDraftSchema = z.object({
  journey_id: z.string().min(1),
  name: z.string().min(1),
  status: z.enum(['draft', 'published', 'archived']).default('draft'),
  folder_path: z.string().min(1).default('Workspace'),
  graph_json: z.object({
    nodes: z.array(GraphNodeSchema).min(1),
    edges: z.array(GraphEdgeSchema).default([])
  }),
  assumptions: z.array(z.string()).default([]),
  questions: z.array(z.string()).default([])
});

const ExplainJourneySchema = z.object({
  summary: z.string().min(1),
  risks: z.array(z.string()).default([]),
  suggestions: z.array(z.string()).default([])
});

const JourneyRevisionInputSchema = z.object({
  journey_id: z.string().min(1).optional(),
  version: z.number().int().positive().optional(),
  name: z.string().min(1).optional(),
  status: z.string().optional(),
  folder_path: z.string().optional(),
  graph_json: z.object({
    nodes: z.array(GraphNodeSchema).default([]),
    edges: z.array(GraphEdgeSchema).default([])
  })
});

const JourneyDraftJsonSchema = {
  type: 'object',
  additionalProperties: false,
  required: ['journey_id', 'name', 'status', 'folder_path', 'graph_json', 'assumptions', 'questions'],
  properties: {
    journey_id: { type: 'string' },
    name: { type: 'string' },
    status: { type: 'string', enum: ['draft', 'published', 'archived'] },
    folder_path: { type: 'string' },
    assumptions: { type: 'array', items: { type: 'string' } },
    questions: { type: 'array', items: { type: 'string' } },
    graph_json: {
      type: 'object',
      additionalProperties: false,
      required: ['nodes', 'edges'],
      properties: {
        nodes: {
          type: 'array',
          minItems: 1,
          items: {
            type: 'object',
            additionalProperties: false,
            required: ['id', 'data'],
            properties: {
              id: { type: 'string' },
              position: {
                type: 'object',
                additionalProperties: false,
                required: ['x', 'y'],
                properties: {
                  x: { type: 'number' },
                  y: { type: 'number' }
                }
              },
              data: {
                type: 'object',
                additionalProperties: false,
                required: ['node_kind'],
                properties: {
                  label: { type: 'string' },
                  node_kind: {
                    type: 'string',
                    enum: ['trigger', 'wait', 'condition', 'action', 'http_call', 'cache_lookup']
                  },
                  event_type: { type: 'string' },
                  wait_minutes: { type: 'number' },
                  manual_release: { type: 'boolean' },
                  cache_dataset_key: { type: 'string' },
                  cache_lookup_key_template: { type: 'string' },
                  cache_target_path: { type: 'string' },
                  cache_on_miss: { type: 'string' },
                  cache_default_json: { type: 'string' },
                  http_method: { type: 'string' },
                  endpoint_id: { type: 'string' },
                  http_url: { type: 'string' },
                  http_headers_json: { type: 'string' },
                  http_body_template: { type: 'string' },
                  http_timeout_ms: { type: 'number' },
                  response_mapping_json: { type: 'string' },
                  condition_key: { type: 'string' },
                  condition_event_type: { type: 'string' },
                  condition_segment_value: { type: 'string' },
                  channel: { type: 'string' },
                  template_id: { type: 'string' }
                }
              }
            }
          }
        },
        edges: {
          type: 'array',
          items: {
            type: 'object',
            additionalProperties: false,
            required: ['id', 'source', 'target'],
            properties: {
              id: { type: 'string' },
              source: { type: 'string' },
              target: { type: 'string' },
              animated: { type: 'boolean' },
              data: {
                type: 'object',
                additionalProperties: false,
                properties: {
                  condition_result: { type: 'string' },
                  edge_type: { type: 'string' },
                  priority: { type: 'number' },
                  delay_minutes: { type: 'number' },
                  expression: { type: 'string' },
                  rate_limit_per_day: { type: 'number' },
                  max_customers_total: { type: 'number' },
                  max_customers_per_day: { type: 'number' }
                }
              }
            }
          }
        }
      }
    }
  }
};

const ExplainJourneyJsonSchema = {
  type: 'object',
  additionalProperties: false,
  required: ['summary', 'risks', 'suggestions'],
  properties: {
    summary: { type: 'string' },
    risks: { type: 'array', items: { type: 'string' } },
    suggestions: { type: 'array', items: { type: 'string' } }
  }
};

function fallbackPosition(index) {
  return { x: 120 + index * 260, y: 140 };
}

function slugify(value) {
  const normalized = String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');
  return normalized || `journey_${crypto.randomUUID().slice(0, 8)}`;
}

function normalizeDraft(rawDraft) {
  const parsed = JourneyDraftSchema.parse(rawDraft);
  return {
    ...parsed,
    status: 'draft',
    graph_json: {
      nodes: parsed.graph_json.nodes.map((node, index) => ({
        ...node,
        position: node.position || fallbackPosition(index)
      })),
      edges: parsed.graph_json.edges
    }
  };
}

function validateDraftAgainstCatalogs(draft, catalogs) {
  const warnings = [];
  const errors = [];
  const eventTypeSet = new Set((catalogs.eventTypes || []).map((item) => String(item)));
  const templateSet = new Set((catalogs.templates || []).map((item) => String(item.template_id)));
  const segmentSet = new Set((catalogs.segments || []).map((item) => String(item)));
  const endpointSet = new Set((catalogs.endpoints || []).map((item) => String(item.endpoint_id)));
  const datasetSet = new Set((catalogs.cacheDatasets || []).map((item) => String(item.dataset_key)));

  for (const node of draft.graph_json.nodes) {
    const data = node?.data || {};
    const nodeKind = String(data.node_kind || '');

    if (nodeKind === 'trigger' && data.event_type && eventTypeSet.size > 0 && !eventTypeSet.has(String(data.event_type))) {
      errors.push(`Bilinmeyen event_type: ${data.event_type}`);
    }

    if (
      nodeKind === 'action' &&
      data.template_id &&
      templateSet.size > 0 &&
      !templateSet.has(String(data.template_id))
    ) {
      errors.push(`Bilinmeyen template_id: ${data.template_id}`);
    }

    if (
      nodeKind === 'condition' &&
      data.condition_segment_value &&
      segmentSet.size > 0 &&
      !segmentSet.has(String(data.condition_segment_value))
    ) {
      warnings.push(`condition_segment_value katalogda bulunamadi: ${data.condition_segment_value}`);
    }

    if (
      nodeKind === 'http_call' &&
      data.endpoint_id &&
      endpointSet.size > 0 &&
      !endpointSet.has(String(data.endpoint_id))
    ) {
      errors.push(`Bilinmeyen endpoint_id: ${data.endpoint_id}`);
    }

    if (
      nodeKind === 'cache_lookup' &&
      data.cache_dataset_key &&
      datasetSet.size > 0 &&
      !datasetSet.has(String(data.cache_dataset_key))
    ) {
      warnings.push(`cache_dataset_key katalogda bulunamadi: ${data.cache_dataset_key}`);
    }
  }

  if (!draft.graph_json.nodes.some((node) => node?.data?.node_kind === 'trigger')) {
    errors.push('Draft en az bir trigger node icermeli');
  }

  if (!draft.graph_json.nodes.some((node) => node?.data?.node_kind === 'action')) {
    warnings.push('Draft action node icermiyor; journey sadece kontrol akisi olabilir');
  }

  return {
    errors,
    warnings,
    valid: errors.length === 0
  };
}

function buildMockDraft(prompt, catalogs) {
  const lowered = String(prompt || '').toLowerCase();
  const defaultEvent = catalogs.eventTypes[0] || 'cart_add';
  const defaultTemplate = catalogs.templates.find((item) => item.channel === 'email')?.template_id || '';
  const eventType = lowered.includes('purchase') ? 'purchase' : lowered.includes('cart') ? defaultEvent : defaultEvent;
  const waitMinutesMatch = lowered.match(/(\d+)\s*(dk|dakika|min|minute)/i);
  const waitMinutes = waitMinutesMatch ? Number(waitMinutesMatch[1]) : 30;
  const channel = lowered.includes('sms') ? 'sms' : 'email';

  return normalizeDraft({
    journey_id: slugify(prompt),
    name: `AI Draft: ${String(prompt || 'Journey').slice(0, 48)}`,
    status: 'draft',
    folder_path: 'Workspace',
    assumptions: [
      `trigger event olarak ${eventType} secildi`,
      `bekleme suresi ${waitMinutes} dakika secildi`,
      channel === 'email' ? 'kanal olarak email secildi' : 'kanal olarak sms secildi'
    ],
    questions: defaultTemplate ? [] : ['Hangi template kullanilsin?'],
    graph_json: {
      nodes: [
        {
          id: 'trigger',
          data: {
            node_kind: 'trigger',
            label: `Trigger: ${eventType}`,
            event_type: eventType
          }
        },
        {
          id: 'wait',
          data: {
            node_kind: 'wait',
            label: `Wait: ${waitMinutes}m`,
            wait_minutes: waitMinutes
          }
        },
        {
          id: 'condition',
          data: {
            node_kind: 'condition',
            label: 'Condition: purchase yok',
            condition_key: 'purchase_exists',
            condition_event_type: 'purchase'
          }
        },
        {
          id: 'action',
          data: {
            node_kind: 'action',
            label: channel === 'email' ? 'Action: Email gonder' : 'Action: SMS gonder',
            channel,
            template_id: defaultTemplate
          }
        }
      ],
      edges: [
        { id: 'e1', source: 'trigger', target: 'wait', animated: true },
        { id: 'e2', source: 'wait', target: 'condition' },
        {
          id: 'e3',
          source: 'condition',
          target: 'action',
          data: { edge_type: 'false', condition_result: 'false' }
        }
      ]
    }
  });
}

function buildMockExplanation(journey) {
  const nodes = Array.isArray(journey?.graph_json?.nodes) ? journey.graph_json.nodes : [];
  const nodeKinds = nodes.map((node) => node?.data?.node_kind || node?.type).filter(Boolean);
  const hasTrigger = nodeKinds.includes('trigger');
  const hasAction = nodeKinds.includes('action');
  return {
    summary: `${journey?.name || journey?.journey_id || 'Journey'} toplam ${nodes.length} node ile akiyor. ${
      hasTrigger ? 'Bir trigger ile basliyor.' : 'Trigger eksik.'
    } ${hasAction ? 'En az bir action node var.' : 'Action node yok.'}`,
    risks: hasTrigger ? [] : ['Journey trigger icermiyor.'],
    suggestions: hasAction ? ['Template ve segment secimlerini kataloglarla eslestir.'] : ['En az bir action node eklemeyi dusun.']
  };
}

function cloneNodes(nodes = []) {
  return nodes.map((node) => ({
    ...node,
    data: { ...(node?.data || {}) },
    position: node?.position ? { ...node.position } : undefined
  }));
}

function cloneEdges(edges = []) {
  return edges.map((edge) => ({
    ...edge,
    data: edge?.data ? { ...edge.data } : undefined
  }));
}

function findEventTypeFromInstruction(instruction, catalogs) {
  const lowered = String(instruction || '').toLowerCase();
  const eventTypes = Array.isArray(catalogs.eventTypes) ? catalogs.eventTypes.map((item) => String(item)) : [];
  const catalogMatch = eventTypes.find((item) => lowered.includes(String(item).toLowerCase()));
  if (catalogMatch) {
    return catalogMatch;
  }
  if (/(sepet|cart)/i.test(lowered)) {
    return eventTypes.find((item) => /cart/i.test(item)) || 'cart_add';
  }
  if (/(satin al|purchase|order)/i.test(lowered)) {
    return eventTypes.find((item) => /purchase|order/i.test(item)) || 'purchase';
  }
  if (/(signup|kayit|register)/i.test(lowered)) {
    return eventTypes.find((item) => /signup|register/i.test(item));
  }
  if (/(login|giris|oturum)/i.test(lowered)) {
    return eventTypes.find((item) => /login|session/i.test(item));
  }
  return '';
}

function findTemplateIdByChannel(catalogs, channel) {
  return catalogs.templates.find((item) => String(item.channel) === String(channel))?.template_id || '';
}

function removeNodeAndReconnect(nodes, edges, nodeId) {
  const incoming = edges.filter((edge) => edge.target === nodeId);
  const outgoing = edges.filter((edge) => edge.source === nodeId);
  const remainingNodes = nodes.filter((node) => node.id !== nodeId);
  const remainingEdges = edges.filter((edge) => edge.source !== nodeId && edge.target !== nodeId);

  if (incoming.length === 1 && outgoing.length === 1) {
    remainingEdges.push({
      id: `edge_${crypto.randomUUID().slice(0, 6)}`,
      source: incoming[0].source,
      target: outgoing[0].target,
      data: outgoing[0].data ? { ...outgoing[0].data } : incoming[0].data ? { ...incoming[0].data } : undefined
    });
  }

  return {
    nodes: remainingNodes,
    edges: remainingEdges
  };
}

function ensureVipExclusionCondition(nodes, edges, catalogs, assumptions) {
  const existing = nodes.find(
    (node) =>
      node?.data?.node_kind === 'condition' &&
      node?.data?.condition_key === 'segment_match' &&
      String(node?.data?.condition_segment_value || '').toLowerCase() === 'vip'
  );
  if (existing) {
    assumptions.push('vip hariç tutma kosulu mevcut journey uzerinde korunuyor');
    return { nodes, edges };
  }

  const segmentValue =
    catalogs.segments.find((item) => String(item).toLowerCase() === 'vip') ||
    catalogs.segments[0] ||
    'vip';
  const actionNode = nodes.find((node) => node?.data?.node_kind === 'action');
  if (!actionNode) {
    assumptions.push('vip hariç tutma kosulu eklenemedi; action node bulunamadi');
    return { nodes, edges };
  }

  const incoming = edges.find((edge) => edge.target === actionNode.id);
  const sourceId = incoming?.source || nodes.find((node) => node.id !== actionNode.id)?.id || actionNode.id;
  const conditionId = `condition_segment_${crypto.randomUUID().slice(0, 6)}`;
  const conditionNode = {
    id: conditionId,
    position: fallbackPosition(nodes.length),
    data: {
      node_kind: 'condition',
      label: 'Condition: VIP degil',
      condition_key: 'segment_match',
      condition_segment_value: String(segmentValue)
    }
  };

  const nextEdges = edges.filter((edge) => !(edge.source === sourceId && edge.target === actionNode.id));
  nextEdges.push({
    id: `edge_${crypto.randomUUID().slice(0, 6)}`,
    source: sourceId,
    target: conditionId
  });
  nextEdges.push({
    id: `edge_${crypto.randomUUID().slice(0, 6)}`,
    source: conditionId,
    target: actionNode.id,
    data: { edge_type: 'false', condition_result: 'false' }
  });

  assumptions.push('vip segmenti icin hariç tutma kosulu eklendi');
  return {
    nodes: [...nodes, conditionNode],
    edges: nextEdges
  };
}

function buildMockRevision(instruction, journey, catalogs) {
  const baseJourney = JourneyRevisionInputSchema.parse(journey || {});
  const baseNodes = Array.isArray(baseJourney.graph_json.nodes) ? baseJourney.graph_json.nodes : [];
  const baseEdges = Array.isArray(baseJourney.graph_json.edges) ? baseJourney.graph_json.edges : [];
  const lowered = String(instruction || '').toLowerCase();
  const waitMinutesMatch = lowered.match(/(\d+)\s*(dk|dakika|min|minute|saat|hour)/i);
  const defaultTemplate = findTemplateIdByChannel(catalogs, 'email');
  const smsTemplate = findTemplateIdByChannel(catalogs, 'sms') || defaultTemplate;

  let revisedNodes = cloneNodes(baseNodes);
  let revisedEdges = cloneEdges(baseEdges);
  const assumptions = [];
  const questions = [];

  const waitNode = revisedNodes.find((node) => node?.data?.node_kind === 'wait');
  if (waitNode && waitMinutesMatch) {
    let waitMinutes = Number(waitMinutesMatch[1]);
    if (/saat|hour/i.test(waitMinutesMatch[2])) {
      waitMinutes *= 60;
    }
    waitNode.data.wait_minutes = waitMinutes;
    waitNode.data.label = `Wait: ${waitMinutes}m`;
    assumptions.push(`bekleme suresi ${waitMinutes} dakikaya guncellendi`);
  }

  const actionNodes = revisedNodes.filter((node) => node?.data?.node_kind === 'action');
  const removeSms = /sms kaldir|sms sil/i.test(lowered);
  const removeEmail = /email kaldir|email sil|mail sil|mail kaldir/i.test(lowered);

  if (actionNodes.length > 0 && /\b(sms|text)\b/i.test(lowered) && !/sms ekle/i.test(lowered) && !removeSms) {
    for (const node of actionNodes) {
      node.data.channel = 'sms';
      if (smsTemplate) {
        node.data.template_id = smsTemplate;
      } else {
        questions.push('SMS icin hangi template kullanilsin?');
      }
      node.data.label = 'Action: SMS gonder';
    }
    assumptions.push('aksiyon kanali sms olarak guncellendi');
  } else if (actionNodes.length > 0 && /\b(email|mail|e-posta)\b/i.test(lowered) && !removeEmail) {
    for (const node of actionNodes) {
      node.data.channel = 'email';
      if (defaultTemplate) {
        node.data.template_id = defaultTemplate;
      } else {
        questions.push('Email icin hangi template kullanilsin?');
      }
      node.data.label = 'Action: Email gonder';
    }
    assumptions.push('aksiyon kanali email olarak guncellendi');
  }

  if (/vip.*haric|vip.*hariç|vip.*disinda|vip.*dışında|vip.*exclude/i.test(lowered)) {
    const updated = ensureVipExclusionCondition(revisedNodes, revisedEdges, catalogs, assumptions);
    revisedNodes = updated.nodes;
    revisedEdges = updated.edges;
  }

  const triggerEventType = findEventTypeFromInstruction(instruction, catalogs);
  if (triggerEventType) {
    const triggerNode = revisedNodes.find((node) => node?.data?.node_kind === 'trigger');
    if (triggerNode && String(triggerNode.data.event_type || '') !== String(triggerEventType)) {
      triggerNode.data.event_type = triggerEventType;
      triggerNode.data.label = `Trigger: ${triggerEventType}`;
      assumptions.push(`trigger event ${triggerEventType} olarak guncellendi`);
    }
  }

  if (lowered.includes('sms ekle')) {
    const lastNodeId = revisedNodes[revisedNodes.length - 1]?.id || 'action';
    const smsNodeId = `action_sms_${crypto.randomUUID().slice(0, 6)}`;
    revisedNodes.push({
      id: smsNodeId,
      position: fallbackPosition(revisedNodes.length),
      data: {
        node_kind: 'action',
        label: 'Action: SMS gonder',
        channel: 'sms',
        template_id: smsTemplate
      }
    });
    revisedEdges.push({
      id: `edge_${crypto.randomUUID().slice(0, 6)}`,
      source: lastNodeId,
      target: smsNodeId
    });
    assumptions.push('sona ek bir sms aksiyonu eklendi');
  }

  if (removeEmail) {
    const emailAction = revisedNodes.find(
      (node) => node?.data?.node_kind === 'action' && String(node?.data?.channel || '').toLowerCase() === 'email'
    );
    if (emailAction) {
      const removed = removeNodeAndReconnect(revisedNodes, revisedEdges, emailAction.id);
      revisedNodes = removed.nodes;
      revisedEdges = removed.edges;
      assumptions.push('email aksiyonu kaldirildi');
    }
  }

  if (removeSms) {
    const smsAction = revisedNodes.find(
      (node) => node?.data?.node_kind === 'action' && String(node?.data?.channel || '').toLowerCase() === 'sms'
    );
    if (smsAction) {
      const removed = removeNodeAndReconnect(revisedNodes, revisedEdges, smsAction.id);
      revisedNodes = removed.nodes;
      revisedEdges = removed.edges;
      assumptions.push('sms aksiyonu kaldirildi');
    }
  }

  if (/condition kaldir|kosul kaldir|koşul kaldır|kosulu sil|koşulu sil/i.test(lowered)) {
    const conditionNode = revisedNodes.find((node) => node?.data?.node_kind === 'condition');
    if (conditionNode) {
      const removed = removeNodeAndReconnect(revisedNodes, revisedEdges, conditionNode.id);
      revisedNodes = removed.nodes;
      revisedEdges = removed.edges;
      assumptions.push('bir condition node kaldirildi');
    }
  }

  if (assumptions.length === 0) {
    assumptions.push('mevcut journey yapisi korunarak kucuk bir revizyon taslagi uretildi');
  }

  return normalizeDraft({
    journey_id: baseJourney.journey_id || slugify(baseJourney.name || 'revised_journey'),
    name: `${baseJourney.name || 'Journey'} (Revize)`,
    status: 'draft',
    folder_path: baseJourney.folder_path || 'Workspace',
    assumptions,
    questions,
    graph_json: {
      nodes: revisedNodes.length > 0 ? revisedNodes : buildMockDraft(instruction, catalogs).graph_json.nodes,
      edges: revisedNodes.length > 0 ? revisedEdges : buildMockDraft(instruction, catalogs).graph_json.edges
    }
  });
}

function buildDraftSystemPrompt() {
  return [
    'You are Eventra AI Copilot.',
    'Generate a safe draft journey graph for a visual customer-journey designer.',
    'Return valid JSON only, matching the provided schema.',
    'Never invent unsupported node kinds.',
    'Prefer draft-safe defaults and include assumptions or follow-up questions when information is missing.',
    'Use only active catalogue values when they are relevant.',
    'Do not publish automatically; status must remain draft.'
  ].join(' ');
}

function buildDraftUserPrompt({ prompt, catalogs, existingJourney }) {
  return JSON.stringify(
    {
      task: 'Generate a journey draft graph JSON for Eventra.',
      user_prompt: prompt,
      catalog_context: catalogs,
      existing_journey: existingJourney || null,
      notes: [
        'Keep the graph simple and executable.',
        'Use trigger -> wait/condition -> action patterns when possible.',
        'If a template or segment is unknown, leave a question in questions[].'
      ]
    },
    null,
    2
  );
}

function buildExplainSystemPrompt() {
  return [
    'You are Eventra AI Copilot.',
    'Explain a customer journey graph in concise Turkish.',
    'Return valid JSON only.',
    'Describe what the journey does, key risks, and practical suggestions.',
    'Do not invent unavailable implementation details.'
  ].join(' ');
}

function buildExplainUserPrompt({ journey, catalogs }) {
  return JSON.stringify(
    {
      task: 'Explain this Eventra journey graph.',
      journey,
      catalog_context: catalogs
    },
    null,
    2
  );
}

function buildRevisionSystemPrompt() {
  return [
    'You are Eventra AI Copilot.',
    'Revise an existing Eventra customer-journey graph based on a user instruction.',
    'Return valid JSON only, matching the provided draft schema.',
    'Keep the output safe, executable, and close to the original journey unless the instruction clearly asks for larger edits.',
    'Use only supported node kinds and prefer preserving journey_id, folder_path, and overall intent.',
    'Status must remain draft.'
  ].join(' ');
}

function buildRevisionUserPrompt({ instruction, journey, catalogs }) {
  return JSON.stringify(
    {
      task: 'Revise this Eventra journey graph according to the user instruction.',
      instruction,
      journey,
      catalog_context: catalogs,
      notes: [
        'Preserve the current journey structure when possible.',
        'If information is missing, record it in questions[].',
        'If the instruction is ambiguous, make minimal safe changes and explain them in assumptions[].'
      ]
    },
    null,
    2
  );
}

async function createOpenAiCompletion({ apiKey, model, systemPrompt, userPrompt, schemaName, schema }) {
  if (!apiKey) {
    throw new Error('OPENAI_API_KEY is required');
  }

  const client = new OpenAI({ apiKey });
  const completion = await client.chat.completions.create({
    model,
    messages: [
      { role: 'system', content: systemPrompt },
      { role: 'user', content: userPrompt }
    ],
    response_format: {
      type: 'json_schema',
      json_schema: {
        name: schemaName,
        strict: true,
        schema
      }
    }
  });

  const message = completion.choices[0]?.message;
  const content = typeof message?.content === 'string' ? message.content : '';
  if (!content) {
    throw new Error('LLM returned an empty response');
  }
  return JSON.parse(content);
}

export async function generateJourneyDraft({
  prompt,
  catalogs,
  existingJourney = null,
  provider = process.env.LLM_PROVIDER || 'openai',
  model = process.env.OPENAI_MODEL || 'gpt-5.4-mini',
  apiKey = process.env.OPENAI_API_KEY
}) {
  const draft =
    provider === 'mock'
      ? buildMockDraft(prompt, catalogs)
      : normalizeDraft(
          await createOpenAiCompletion({
            apiKey,
            model,
            systemPrompt: buildDraftSystemPrompt(),
            userPrompt: buildDraftUserPrompt({ prompt, catalogs, existingJourney }),
            schemaName: 'eventra_journey_draft',
            schema: JourneyDraftJsonSchema
          })
        );

  return {
    draft,
    provider,
    validation: validateDraftAgainstCatalogs(draft, catalogs)
  };
}

export async function explainJourney({
  journey,
  catalogs,
  provider = process.env.LLM_PROVIDER || 'openai',
  model = process.env.OPENAI_MODEL || 'gpt-5.4-mini',
  apiKey = process.env.OPENAI_API_KEY
}) {
  const explanation =
    provider === 'mock'
      ? buildMockExplanation(journey)
      : ExplainJourneySchema.parse(
          await createOpenAiCompletion({
            apiKey,
            model,
            systemPrompt: buildExplainSystemPrompt(),
            userPrompt: buildExplainUserPrompt({ journey, catalogs }),
            schemaName: 'eventra_journey_explain',
            schema: ExplainJourneyJsonSchema
          })
        );

  return {
    explanation,
    provider
  };
}

export async function reviseJourney({
  instruction,
  journey,
  catalogs,
  provider = process.env.LLM_PROVIDER || 'openai',
  model = process.env.OPENAI_MODEL || 'gpt-5.4-mini',
  apiKey = process.env.OPENAI_API_KEY
}) {
  const draft =
    provider === 'mock'
      ? buildMockRevision(instruction, journey, catalogs)
      : normalizeDraft(
          await createOpenAiCompletion({
            apiKey,
            model,
            systemPrompt: buildRevisionSystemPrompt(),
            userPrompt: buildRevisionUserPrompt({ instruction, journey, catalogs }),
            schemaName: 'eventra_journey_revision',
            schema: JourneyDraftJsonSchema
          })
        );

  return {
    draft,
    provider,
    validation: validateDraftAgainstCatalogs(draft, catalogs)
  };
}
