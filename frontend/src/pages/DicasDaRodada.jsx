import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

const STREAM_EVENTS = [
  "status",
  "progress",
  "tool_call",
  "tool_result",
  "report_delta",
  "final_report",
  "warning",
  "error",
  "done",
];

const HISTORY_LIMIT = 100;

function DicasDaRodada() {
  const [loading, setLoading] = useState(true);
  const [actionLoading, setActionLoading] = useState(false);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [error, setError] = useState(null);
  const [rodada, setRodada] = useState(null);
  const [report, setReport] = useState(null);
  const [activeRun, setActiveRun] = useState(null);
  const [events, setEvents] = useState([]);
  const [history, setHistory] = useState([]);
  const [historyRounds, setHistoryRounds] = useState([]);
  const [selectedHistoryRodada, setSelectedHistoryRodada] = useState(null);
  const [historyCollapsed, setHistoryCollapsed] = useState(false);
  const [liveReport, setLiveReport] = useState("");
  const [selectedReportId, setSelectedReportId] = useState(null);
  const [copyState, setCopyState] = useState("idle");
  const [deleteLoading, setDeleteLoading] = useState(false);
  const eventSourceRef = useRef(null);
  const connectedRunRef = useRef(null);

  const closeStream = useCallback(() => {
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }
    connectedRunRef.current = null;
  }, []);

  const fetchHistory = useCallback(async (round = null) => {
    setHistoryLoading(true);
    try {
      const params = new URLSearchParams({ limit: String(HISTORY_LIMIT) });
      if (Number.isFinite(round)) params.set("rodada", String(round));
      const res = await fetch(`/api/dicas-da-rodada/history?${params.toString()}`);
      if (!res.ok) throw new Error("Falha ao carregar histórico");
      const data = await res.json();
      setHistory(data.reports || []);
      setHistoryRounds(data.rodadas || []);
    } catch {
      setHistory([]);
      setHistoryRounds([]);
    } finally {
      setHistoryLoading(false);
    }
  }, []);

  const fetchStatus = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch("/api/dicas-da-rodada");
      if (!res.ok) throw new Error("Falha ao carregar Dicas da Rodada");
      const data = await res.json();
      setRodada(data.rodada);
      setReport(data.report);
      setActiveRun(data.active_run);
      setSelectedReportId(data.report?.report_id || null);
      if (!data.active_run) {
        closeStream();
        setLiveReport("");
      }
      return data;
    } catch (err) {
      setError(err.message);
      return null;
    } finally {
      setLoading(false);
    }
  }, [closeStream]);

  const attachStream = useCallback(
    (run) => {
      if (!run?.run_id || connectedRunRef.current === run.run_id) return;
      closeStream();
      setEvents([]);
      setLiveReport("");
      connectedRunRef.current = run.run_id;
      const source = new EventSource(
        `/api/dicas-da-rodada/runs/${run.run_id}/stream`
      );
      eventSourceRef.current = source;

      const handleEvent = (event) => {
        let payload;
        try {
          payload = JSON.parse(event.data);
        } catch {
          return;
        }

        setEvents((prev) => {
          const key = `${event.lastEventId || payload.id || prev.length + 1}-${payload.type}-${payload.created_at}`;
          if (prev.some((item) => item.key === key)) return prev;
          return [...prev, { ...payload, key }];
        });

        if (payload.type === "report_delta" && payload.data?.text) {
          setLiveReport((prev) => `${prev}${payload.data.text}`);
        }
        if (payload.type === "final_report" && payload.data?.report) {
          setReport(payload.data.report);
          setSelectedReportId(payload.data.report.report_id || null);
          setLiveReport(payload.data.report.report_markdown || "");
          setSelectedHistoryRodada(payload.data.report.rodada || null);
          fetchHistory(payload.data.report.rodada || null);
        }
        if (payload.type === "error") {
          setError(payload.message || "Falha na geração");
        }
        if (payload.type === "done") {
          source.close();
          eventSourceRef.current = null;
          connectedRunRef.current = null;
          fetchStatus();
        }
      };

      STREAM_EVENTS.forEach((eventName) => {
        source.addEventListener(eventName, handleEvent);
      });
    },
    [closeStream, fetchHistory, fetchStatus]
  );

  useEffect(() => {
    fetchStatus().then((data) => {
      if (data?.active_run) attachStream(data.active_run);
      const initialRound = data?.rodada ?? null;
      setSelectedHistoryRodada(initialRound);
      fetchHistory(initialRound);
    });
    return closeStream;
  }, [fetchStatus, fetchHistory, attachStream, closeStream]);

  const startGeneration = useCallback(
    async (mode) => {
      setActionLoading(true);
      setError(null);
      setSelectedReportId(null);
      try {
        const endpoint =
          mode === "regenerate"
            ? "/api/dicas-da-rodada/regenerate"
            : "/api/dicas-da-rodada/generate";
        const res = await fetch(endpoint, { method: "POST" });
        if (!res.ok) throw new Error("Falha ao iniciar geração");
        const data = await res.json();
        setRodada(data.rodada);
        setReport(data.report || null);
        setSelectedReportId(data.report?.report_id || null);
        setActiveRun(data.run || null);
        if (data.run) attachStream(data.run);
        setSelectedHistoryRodada(data.rodada || null);
        fetchHistory(data.rodada || null);
      } catch (err) {
        setError(err.message);
      } finally {
        setActionLoading(false);
      }
    },
    [attachStream, fetchHistory]
  );

  const loadHistoryReport = useCallback(async (reportId) => {
    setError(null);
    try {
      const res = await fetch(
        `/api/dicas-da-rodada/history/${encodeURIComponent(reportId)}`
      );
      if (!res.ok) throw new Error("Falha ao carregar relatório salvo");
      const data = await res.json();
      setReport(data);
      setLiveReport("");
      setSelectedReportId(data.report_id || reportId);
      if (data.rodada) setSelectedHistoryRodada(data.rodada);
    } catch (err) {
      setError(err.message);
    }
  }, []);

  const hasActiveRun =
    activeRun?.status === "queued" || activeRun?.status === "running";
  const displayedRodada = report?.rodada || rodada;
  const reportMarkdown = liveReport || report?.report_markdown || "";
  const hasExecutionContent = hasActiveRun || events.length > 0;
  const currentReportId = selectedReportId || report?.report_id || null;

  const copyMarkdown = useCallback(async () => {
    if (!reportMarkdown) return;
    setError(null);
    try {
      await copyTextToClipboard(reportMarkdown);
      setCopyState("copied");
      window.setTimeout(() => setCopyState("idle"), 1600);
    } catch {
      setCopyState("idle");
      setError("Não foi possível copiar o Markdown.");
    }
  }, [reportMarkdown]);

  const trashReport = useCallback(async () => {
    if (!currentReportId) return;
    const confirmed = window.confirm("Excluir este relatório salvo?");
    if (!confirmed) return;

    setDeleteLoading(true);
    setError(null);
    try {
      const res = await fetch(
        `/api/dicas-da-rodada/history/${encodeURIComponent(currentReportId)}`,
        { method: "DELETE" }
      );
      if (!res.ok) throw new Error("Falha ao excluir relatório");
      const payload = await res.json();
      if (selectedReportId === currentReportId || report?.report_id === currentReportId) {
        setReport(null);
        setLiveReport("");
        setSelectedReportId(null);
      }
      const nextRound = payload.rodada || selectedHistoryRodada || rodada || null;
      await fetchHistory(nextRound);
      await fetchStatus();
    } catch (err) {
      setError(err.message);
    } finally {
      setDeleteLoading(false);
    }
  }, [
    currentReportId,
    fetchHistory,
    fetchStatus,
    report?.report_id,
    rodada,
    selectedHistoryRodada,
    selectedReportId,
  ]);

  const roundOptions = useMemo(() => {
    const options = [selectedHistoryRodada, rodada, ...historyRounds].filter(
      (value) => Number.isFinite(value)
    );
    return Array.from(new Set(options)).sort((a, b) => b - a);
  }, [historyRounds, rodada, selectedHistoryRodada]);

  const historyGroups = useMemo(() => {
    if (selectedHistoryRodada) {
      return [{ rodada: selectedHistoryRodada, reports: history }];
    }
    const grouped = history.reduce((acc, item) => {
      const key = item.rodada || "Sem rodada";
      if (!acc.has(key)) acc.set(key, []);
      acc.get(key).push(item);
      return acc;
    }, new Map());
    return Array.from(grouped.entries()).map(([groupRodada, reports]) => ({
      rodada: groupRodada,
      reports,
    }));
  }, [history, selectedHistoryRodada]);

  const handleHistoryRoundChange = useCallback(
    (event) => {
      const nextRound = event.target.value ? Number(event.target.value) : null;
      setSelectedHistoryRodada(nextRound);
      fetchHistory(nextRound);
    },
    [fetchHistory]
  );

  const statusLabel = useMemo(() => {
    if (activeRun?.status === "queued") return "Na fila";
    if (activeRun?.status === "running") return "Gerando";
    if (report) return "Pronto";
    return "Sem relatório";
  }, [activeRun, report]);

  return (
    <div style={{ display: "grid", gap: "1.25rem", minHeight: 0 }}>
      <header
        style={{
          display: "flex",
          alignItems: "flex-start",
          justifyContent: "space-between",
          gap: "1rem",
          flexWrap: "wrap",
        }}
      >
        <div>
          <h1
            style={{
              fontFamily: "var(--font-display)",
              fontSize: "1.75rem",
              fontWeight: 700,
              color: "var(--text-primary)",
              letterSpacing: 0,
            }}
          >
            Dicas da Rodada
          </h1>
          <div
            style={{
              fontFamily: "var(--font-display)",
              fontSize: "0.85rem",
              color: "var(--text-secondary)",
              marginTop: "0.25rem",
            }}
          >
            Rodada {displayedRodada || "-"} · {statusLabel}
          </div>
        </div>

        <button
          onClick={() => startGeneration(report ? "regenerate" : "generate")}
          disabled={actionLoading || hasActiveRun}
          style={{
            padding: "0.6rem 0.9rem",
            border: "none",
            borderRadius: "var(--radius-sm)",
            background:
              actionLoading || hasActiveRun ? "var(--bg-tertiary)" : "var(--orange)",
            color: actionLoading || hasActiveRun ? "var(--text-muted)" : "white",
            fontWeight: 700,
            boxShadow:
              actionLoading || hasActiveRun ? "none" : "0 4px 14px var(--orange-glow)",
            transition: "all var(--transition)",
          }}
        >
          {report ? "Regenerar" : "Gerar relatório"}
        </button>
      </header>

      {error && (
        <div
          style={{
            padding: "0.85rem 1rem",
            background: "rgba(239, 68, 68, 0.08)",
            border: "1px solid rgba(239, 68, 68, 0.28)",
            borderRadius: "var(--radius-sm)",
            color: "#EF4444",
            fontSize: "0.9rem",
          }}
        >
          {error}
        </div>
      )}

      <section
        className="dicas-layout"
        style={{
          display: "grid",
          gridTemplateColumns: historyCollapsed
            ? "54px minmax(0, 1fr)"
            : "300px minmax(0, 1fr)",
          gap: "1rem",
          alignItems: "start",
          minHeight: 0,
          transition: "grid-template-columns var(--transition)",
        }}
      >
        <HistorySidebar
          collapsed={historyCollapsed}
          onToggle={() => setHistoryCollapsed((current) => !current)}
          historyLoading={historyLoading}
          historyGroups={historyGroups}
          roundOptions={roundOptions}
          selectedRound={selectedHistoryRodada}
          selectedReportId={selectedReportId}
          onRoundChange={handleHistoryRoundChange}
          onSelectReport={loadHistoryReport}
        />

        <article
          className="dicas-report-shell"
          style={{
            height: "calc(100vh - 13.5rem)",
            minHeight: "34rem",
            background: "var(--bg-card)",
            border: "1px solid var(--border)",
            borderRadius: "var(--radius-lg)",
            boxShadow: "var(--shadow-md)",
            overflow: "hidden",
            display: "flex",
            flexDirection: "column",
          }}
        >
          <div
            style={{
              padding: "0.9rem 1rem",
              borderBottom: "1px solid var(--border)",
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              gap: "1rem",
              background: "var(--bg-secondary)",
              flexShrink: 0,
            }}
          >
            <span
              style={{
                fontFamily: "var(--font-display)",
                fontWeight: 700,
                color: "var(--text-primary)",
              }}
            >
              Relatório
            </span>
            <div
              style={{
                display: "flex",
                alignItems: "center",
                justifyContent: "flex-end",
                gap: "0.5rem",
                flexWrap: "wrap",
              }}
            >
              <span style={{ color: "var(--text-muted)", fontSize: "0.78rem" }}>
                {report?.generated_at
                  ? new Date(report.generated_at).toLocaleString("pt-BR")
                  : ""}
              </span>
              <ReportActionButton
                onClick={copyMarkdown}
                disabled={!reportMarkdown}
                title="Copiar Markdown"
              >
                {copyState === "copied" ? "Copiado" : "Copiar Markdown"}
              </ReportActionButton>
              <ReportActionButton
                onClick={trashReport}
                disabled={!currentReportId || deleteLoading || hasActiveRun}
                tone="danger"
                title="Excluir relatório"
              >
                {deleteLoading ? "Excluindo" : "Excluir"}
              </ReportActionButton>
            </div>
          </div>

          <div
            className="dicas-report-scroll"
            style={{
              flex: 1,
              minHeight: 0,
              overflowY: "auto",
              padding: "1.25rem",
              scrollbarGutter: "stable",
            }}
          >
            {loading ? (
              <LoadingState />
            ) : (
              <>
                {hasExecutionContent && (
                  <ExecutionPanel activeRun={activeRun} events={events} />
                )}
                {reportMarkdown ? (
                  <MarkdownReport markdown={reportMarkdown} />
                ) : hasExecutionContent ? null : (
                  <EmptyState />
                )}
              </>
            )}
          </div>
        </article>
      </section>

      <style>{`
        @media (max-width: 980px) {
          .dicas-layout {
            grid-template-columns: 1fr !important;
          }

          .dicas-report-shell,
          .dicas-history-shell {
            height: auto !important;
            min-height: 0 !important;
          }

          .dicas-report-scroll {
            max-height: 72vh;
          }

          .dicas-history-scroll {
            max-height: 28rem;
          }
        }
      `}</style>
    </div>
  );
}

function ReportActionButton({
  children,
  disabled = false,
  onClick,
  title,
  tone = "default",
}) {
  const isDanger = tone === "danger";
  return (
    <button
      type="button"
      title={title}
      onClick={onClick}
      disabled={disabled}
      style={{
        minHeight: "2rem",
        padding: "0 0.65rem",
        border: `1px solid ${isDanger ? "rgba(239, 68, 68, 0.36)" : "var(--border)"}`,
        borderRadius: "var(--radius-sm)",
        background: disabled
          ? "var(--bg-tertiary)"
          : isDanger
            ? "rgba(239, 68, 68, 0.08)"
            : "var(--bg-card)",
        color: disabled
          ? "var(--text-muted)"
          : isDanger
            ? "#EF4444"
            : "var(--text-primary)",
        fontSize: "0.78rem",
        fontWeight: 800,
        transition: "border-color var(--transition), background var(--transition)",
        whiteSpace: "nowrap",
      }}
    >
      {children}
    </button>
  );
}

async function copyTextToClipboard(text) {
  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(text);
    return;
  }

  const textarea = document.createElement("textarea");
  textarea.value = text;
  textarea.setAttribute("readonly", "");
  textarea.style.position = "fixed";
  textarea.style.opacity = "0";
  document.body.appendChild(textarea);
  textarea.select();
  const copied = document.execCommand("copy");
  document.body.removeChild(textarea);
  if (!copied) {
    throw new Error("Copy command failed");
  }
}

function HistorySidebar({
  collapsed,
  onToggle,
  historyLoading,
  historyGroups,
  roundOptions,
  selectedRound,
  selectedReportId,
  onRoundChange,
  onSelectReport,
}) {
  const shellStyle = {
    height: "calc(100vh - 13.5rem)",
    minHeight: "34rem",
    background: "var(--bg-card)",
    border: "1px solid var(--border)",
    borderRadius: "var(--radius-lg)",
    boxShadow: "var(--shadow-md)",
    overflow: "hidden",
    display: "flex",
    flexDirection: "column",
    minWidth: 0,
  };

  if (collapsed) {
    return (
      <aside
        className="dicas-history-shell dicas-history-shell--collapsed"
        style={{
          ...shellStyle,
          alignItems: "center",
          padding: "0.55rem 0.4rem",
          gap: "0.75rem",
        }}
      >
        <button
          type="button"
          aria-label="Expandir relatórios"
          title="Expandir relatórios"
          onClick={onToggle}
          style={{
            width: "34px",
            height: "34px",
            border: "1px solid var(--border)",
            borderRadius: "var(--radius-sm)",
            background: "var(--bg-secondary)",
            color: "var(--text-primary)",
            fontWeight: 800,
            display: "grid",
            placeItems: "center",
            flexShrink: 0,
          }}
        >
          <span aria-hidden="true">&gt;</span>
        </button>
        <div
          style={{
            writingMode: "vertical-rl",
            transform: "rotate(180deg)",
            color: "var(--text-secondary)",
            fontFamily: "var(--font-display)",
            fontSize: "0.76rem",
            fontWeight: 700,
            letterSpacing: 0,
            whiteSpace: "nowrap",
          }}
        >
          Relatórios
        </div>
      </aside>
    );
  }

  return (
    <aside className="dicas-history-shell" style={shellStyle}>
      <div
        style={{
          padding: "0.75rem",
          borderBottom: "1px solid var(--border)",
          background: "var(--bg-secondary)",
          display: "grid",
          gap: "0.75rem",
          flexShrink: 0,
        }}
      >
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            gap: "0.75rem",
          }}
        >
          <span
            style={{
              fontFamily: "var(--font-display)",
              fontWeight: 700,
              color: "var(--text-primary)",
            }}
          >
            Relatórios
          </span>
          <button
            type="button"
            aria-label="Recolher relatórios"
            title="Recolher relatórios"
            onClick={onToggle}
            style={{
              width: "32px",
              height: "32px",
              border: "1px solid var(--border)",
              borderRadius: "var(--radius-sm)",
              background: "var(--bg-card)",
              color: "var(--text-primary)",
              fontWeight: 800,
              display: "grid",
              placeItems: "center",
              flexShrink: 0,
            }}
          >
            <span aria-hidden="true">&lt;</span>
          </button>
        </div>

        <label
          style={{
            display: "grid",
            gap: "0.35rem",
            color: "var(--text-secondary)",
            fontSize: "0.76rem",
            fontWeight: 700,
          }}
        >
          Rodada
          <select
            value={selectedRound || ""}
            onChange={onRoundChange}
            disabled={roundOptions.length === 0}
            style={{
              width: "100%",
              minHeight: "2.25rem",
              border: "1px solid var(--border)",
              borderRadius: "var(--radius-sm)",
              background: "var(--bg-card)",
              color: "var(--text-primary)",
              padding: "0 0.65rem",
              fontWeight: 700,
            }}
          >
            {roundOptions.length === 0 ? (
              <option value="">Sem rodadas salvas</option>
            ) : (
              roundOptions.map((round) => (
                <option key={round} value={round}>
                  Rodada {round}
                </option>
              ))
            )}
          </select>
        </label>
      </div>

      <div
        className="dicas-history-scroll"
        style={{
          flex: 1,
          minHeight: 0,
          overflowY: "auto",
          padding: "0.75rem",
          display: "grid",
          alignContent: "start",
          gap: "0.8rem",
          scrollbarGutter: "stable",
        }}
      >
        {historyLoading ? (
          <LoadingState compact />
        ) : historyGroups.length === 0 ? (
          <HistoryEmptyState selectedRound={selectedRound} />
        ) : (
          historyGroups.map((group) => (
            <div key={group.rodada} style={{ display: "grid", gap: "0.55rem" }}>
              <div
                style={{
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "space-between",
                  gap: "0.5rem",
                  color: "var(--text-secondary)",
                  fontSize: "0.78rem",
                  fontWeight: 800,
                }}
              >
                <span>Rodada {group.rodada}</span>
                <span style={{ color: "var(--text-muted)", fontWeight: 700 }}>
                  {group.reports.length}
                </span>
              </div>
              {group.reports.length === 0 ? (
                <HistoryEmptyState selectedRound={group.rodada} />
              ) : (
                group.reports.map((item) => (
                  <HistoryReportItem
                    key={item.report_id}
                    item={item}
                    active={selectedReportId === item.report_id}
                    onSelect={onSelectReport}
                  />
                ))
              )}
            </div>
          ))
        )}
      </div>
    </aside>
  );
}

function HistoryEmptyState({ selectedRound }) {
  return (
    <div
      style={{
        color: "var(--text-muted)",
        fontSize: "0.85rem",
        lineHeight: 1.45,
      }}
    >
      {selectedRound
        ? "Nenhum relatório salvo para esta rodada."
        : "Nenhum relatório salvo."}
    </div>
  );
}

function LoadingState({ compact = false }) {
  return (
    <div
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: "0.75rem",
        color: "var(--text-muted)",
        fontSize: compact ? "0.85rem" : "0.95rem",
      }}
    >
      <div
        style={{
          width: compact ? "16px" : "20px",
          height: compact ? "16px" : "20px",
          border: "2px solid var(--border)",
          borderTopColor: "var(--orange)",
          borderRadius: "50%",
          animation: "spin 0.8s linear infinite",
        }}
      />
      Carregando...
      <style>{`
        @keyframes spin {
          to { transform: rotate(360deg); }
        }
      `}</style>
    </div>
  );
}

function EmptyState() {
  return (
    <div
      style={{
        minHeight: "20rem",
        display: "grid",
        placeItems: "center",
        color: "var(--text-muted)",
        textAlign: "center",
        fontSize: "0.95rem",
      }}
    >
      Nenhum relatório gerado ainda.
    </div>
  );
}

function ExecutionPanel({ activeRun, events }) {
  const recentEvents = events.slice(-40);
  const activeStatus = activeRun?.status;

  return (
    <div
      style={{
        marginBottom: "1.25rem",
        paddingBottom: "1.1rem",
        borderBottom: "1px solid var(--border)",
        display: "grid",
        gap: "0.85rem",
      }}
    >
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          gap: "0.75rem",
          alignItems: "center",
          flexWrap: "wrap",
        }}
      >
        <div
          style={{
            fontFamily: "var(--font-display)",
            fontWeight: 700,
            color: "var(--text-primary)",
          }}
        >
          Execução
        </div>
        {activeStatus && <StatusPill status={activeStatus} />}
      </div>

      {activeRun?.run_id && (
        <div
          style={{
            color: "var(--text-muted)",
            fontFamily: "var(--font-mono)",
            fontSize: "0.72rem",
            overflowWrap: "anywhere",
          }}
        >
          {activeRun.run_id}
        </div>
      )}

      <div
        style={{
          maxHeight: "18rem",
          overflowY: "auto",
          display: "grid",
          gap: "0.75rem",
          paddingRight: "0.25rem",
          scrollbarGutter: "stable",
        }}
      >
        {recentEvents.length === 0 ? (
          <div style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>
            Aguardando eventos...
          </div>
        ) : (
          recentEvents.map((event) => (
            <TimelineEvent key={event.key} event={event} />
          ))
        )}
      </div>
    </div>
  );
}

function StatusPill({ status }) {
  const label = status === "queued" ? "Na fila" : "Gerando";
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        minHeight: "1.75rem",
        padding: "0 0.65rem",
        borderRadius: "var(--radius-sm)",
        background: "rgba(249, 115, 22, 0.1)",
        color: "var(--orange)",
        fontFamily: "var(--font-display)",
        fontWeight: 700,
        fontSize: "0.78rem",
      }}
    >
      {label}
    </span>
  );
}

function HistoryReportItem({ item, active, onSelect }) {
  return (
    <button
      type="button"
      onClick={() => onSelect(item.report_id)}
      style={{
        width: "100%",
        textAlign: "left",
        border: `1px solid ${active ? "rgba(249, 115, 22, 0.42)" : "var(--border)"}`,
        background: active ? "rgba(249, 115, 22, 0.08)" : "var(--bg-secondary)",
        borderRadius: "var(--radius-sm)",
        padding: "0.75rem",
        display: "grid",
        gap: "0.35rem",
        color: "var(--text-primary)",
        transition: "border-color var(--transition), background var(--transition)",
      }}
    >
      <span
        style={{
          fontFamily: "var(--font-display)",
          fontWeight: 700,
          fontSize: "0.86rem",
          lineHeight: 1.25,
          overflowWrap: "anywhere",
        }}
      >
        {item.title || "Dicas da Rodada"}
      </span>
      <span style={{ color: "var(--text-secondary)", fontSize: "0.78rem" }}>
        {item.season_year ? `Temporada ${item.season_year} · ` : ""}
        Rodada {item.rodada || "-"}
      </span>
      <span style={{ color: "var(--text-muted)", fontSize: "0.72rem" }}>
        {formatDate(item.generated_at)}
      </span>
    </button>
  );
}

function MarkdownReport({ markdown }) {
  return (
    <div
      style={{
        color: "var(--text-primary)",
        fontSize: "0.95rem",
        lineHeight: 1.75,
        overflowWrap: "anywhere",
      }}
    >
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        components={{
          h1: ({ children }) => (
            <h2
              style={{
                fontFamily: "var(--font-display)",
                fontSize: "1.25rem",
                margin: "1rem 0 0.45rem",
                color: "var(--text-primary)",
                letterSpacing: 0,
              }}
            >
              {children}
            </h2>
          ),
          h2: ({ children }) => (
            <h3
              style={{
                fontFamily: "var(--font-display)",
                fontSize: "1.05rem",
                margin: "0.9rem 0 0.35rem",
                color: "var(--text-primary)",
                letterSpacing: 0,
              }}
            >
              {children}
            </h3>
          ),
          p: ({ children }) => (
            <p style={{ margin: "0.4rem 0", color: "var(--text-primary)" }}>
              {children}
            </p>
          ),
          ul: ({ children }) => (
            <ul style={{ margin: "0.5rem 0 0.5rem 1.2rem" }}>{children}</ul>
          ),
          ol: ({ children }) => (
            <ol style={{ margin: "0.5rem 0 0.5rem 1.2rem" }}>{children}</ol>
          ),
          li: ({ children }) => (
            <li style={{ margin: "0.25rem 0" }}>{children}</li>
          ),
          strong: ({ children }) => (
            <strong style={{ color: "var(--orange)" }}>{children}</strong>
          ),
          table: ({ children }) => (
            <div
              style={{
                overflowX: "auto",
                margin: "0.9rem 0",
                border: "1px solid var(--border)",
                borderRadius: "var(--radius-sm)",
              }}
            >
              <table
                style={{
                  width: "100%",
                  minWidth: "760px",
                  borderCollapse: "collapse",
                  fontSize: "0.86rem",
                  lineHeight: 1.45,
                }}
              >
                {children}
              </table>
            </div>
          ),
          th: ({ children }) => (
            <th
              style={{
                padding: "0.65rem",
                textAlign: "left",
                background: "var(--bg-secondary)",
                borderBottom: "1px solid var(--border)",
                color: "var(--text-primary)",
                fontFamily: "var(--font-display)",
                fontWeight: 700,
              }}
            >
              {children}
            </th>
          ),
          td: ({ children }) => (
            <td
              style={{
                padding: "0.6rem 0.65rem",
                borderTop: "1px solid var(--border)",
                color: "var(--text-primary)",
                verticalAlign: "top",
              }}
            >
              {children}
            </td>
          ),
        }}
      >
        {markdown}
      </ReactMarkdown>
    </div>
  );
}

function TimelineEvent({ event }) {
  const color =
    event.type === "error"
      ? "#EF4444"
      : event.type === "warning"
        ? "#F59E0B"
        : event.type === "tool_result"
          ? "#22C55E"
          : "var(--orange)";

  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "0.65rem 1fr",
        gap: "0.65rem",
        alignItems: "start",
      }}
    >
      <span
        style={{
          width: "0.55rem",
          height: "0.55rem",
          marginTop: "0.45rem",
          borderRadius: "50%",
          background: color,
          boxShadow: `0 0 0 4px ${event.type === "error" ? "rgba(239,68,68,0.12)" : "rgba(249,115,22,0.12)"}`,
        }}
      />
      <div>
        <div
          style={{
            color: "var(--text-primary)",
            fontSize: "0.84rem",
            fontWeight: 600,
            overflowWrap: "anywhere",
          }}
        >
          {event.message}
        </div>
        <div style={{ color: "var(--text-muted)", fontSize: "0.72rem" }}>
          {event.created_at
            ? new Date(event.created_at).toLocaleTimeString("pt-BR")
            : ""}
        </div>
      </div>
    </div>
  );
}

function formatDate(value) {
  if (!value) return "";
  return new Date(value).toLocaleString("pt-BR");
}

export default DicasDaRodada;
