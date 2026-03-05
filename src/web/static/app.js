function signalsApp() {
  return {
    // Data
    accounts: [],
    stats: { total: 0, high: 0, medium: 0, researched: 0, labeled: 0 },
    selected: [],
    search: "",
    tierFilter: "",
    labelFilter: "",
    sourceFilter: "",
    sortBy: "score",
    sortDir: "desc",
    page: 1,
    totalPages: 1,
    expandedId: null,
    bulkLabel: "",

    // Pipeline modal
    showPipelineModal: false,
    pipelineRunning: false,
    pipelineStages: [],

    // Rubric modal
    showRubricModal: false,
    rubricData: null,

    // CSV Import/Export
    csvUploading: false,

    // Theme
    theme: localStorage.getItem("signals_theme") || "dark",

    async init() {
      // Apply saved theme
      this._applyTheme(this.theme);
      await this.loadAccounts();
    },

    toggleTheme() {
      this.theme = this.theme === "dark" ? "light" : "dark";
      this._applyTheme(this.theme);
      localStorage.setItem("signals_theme", this.theme);
    },

    _applyTheme(t) {
      if (t === "light") {
        document.documentElement.setAttribute("data-theme", "light");
      } else {
        document.documentElement.removeAttribute("data-theme");
      }
    },

    // --- CSV Export ---
    exportCsv() {
      const params = new URLSearchParams({
        tier: this.tierFilter,
        label: this.labelFilter,
        q: this.search,
        source: this.sourceFilter,
      });
      window.open(`/api/export/csv?${params}`, "_blank");
    },

    // --- CSV Import ---
    async handleCsvUpload(event) {
      const file = event.target.files[0];
      if (!file) return;
      if (!file.name.toLowerCase().endsWith(".csv")) {
        alert("Please select a CSV file");
        event.target.value = "";
        return;
      }
      this.csvUploading = true;
      try {
        const formData = new FormData();
        formData.append("file", file);
        const resp = await fetch("/api/v1/upload/csv", {
          method: "POST",
          body: formData,
        });
        const data = await resp.json();
        if (!resp.ok) {
          const msg = data.detail?.message || data.detail || "Upload failed";
          alert("Upload error: " + msg);
        } else {
          const count = data.row_count || 0;
          const errors = (data.validation_errors || []).length;
          alert(
            `Imported ${count} companies (batch: ${data.batch_id})` +
              (errors > 0 ? `\n${errors} validation warning(s)` : ""),
          );
          await this.loadAccounts();
        }
      } catch (err) {
        alert("Upload failed: " + err.message);
      } finally {
        this.csvUploading = false;
        event.target.value = "";
      }
    },

    async loadAccounts() {
      const params = new URLSearchParams({
        page: this.page,
        per_page: 50,
        sort: this.sortBy,
        dir: this.sortDir,
        tier: this.tierFilter,
        label: this.labelFilter,
        q: this.search,
        source: this.sourceFilter,
      });
      try {
        const resp = await fetch(`/api/accounts?${params}`);
        const data = await resp.json();
        this.accounts = data.items || [];
        this.totalPages = data.pages || 1;

        // Compute stats from total data
        this.stats.total = data.total || 0;
        this.stats.high = this.accounts.filter((a) => a.tier === "high").length;
        this.stats.medium = this.accounts.filter(
          (a) => a.tier === "medium",
        ).length;
        this.stats.researched = this.accounts.filter(
          (a) => a.research_status === "completed",
        ).length;
        this.stats.labeled = this.accounts.filter((a) => a.labels).length;

        // Load full stats once
        if (
          this.page === 1 &&
          !this.search &&
          !this.tierFilter &&
          !this.labelFilter &&
          !this.sourceFilter
        ) {
          this._loadFullStats();
        }
      } catch (e) {
        console.error("Failed to load accounts:", e);
      }
    },

    async loadRubric() {
      if (this.rubricData) return; // already loaded
      try {
        const resp = await fetch("/api/scoring/rubric");
        this.rubricData = await resp.json();
      } catch (e) {
        console.error("Failed to load rubric:", e);
      }
    },

    async _loadFullStats() {
      try {
        // Get high tier count
        const highResp = await fetch("/api/accounts?tier=high&per_page=1");
        const highData = await highResp.json();
        this.stats.high = highData.total || 0;

        const medResp = await fetch("/api/accounts?tier=medium&per_page=1");
        const medData = await medResp.json();
        this.stats.medium = medData.total || 0;
      } catch (e) {}
    },

    setSort(col) {
      if (this.sortBy === col) {
        this.sortDir = this.sortDir === "desc" ? "asc" : "desc";
      } else {
        this.sortBy = col;
        this.sortDir = col === "score" ? "desc" : "asc";
      }
      this.loadAccounts();
    },

    toggleAll(event) {
      if (event.target.checked) {
        this.selected = this.accounts.map((a) => a.account_id);
      } else {
        this.selected = [];
      }
    },

    toggleExpand(id) {
      this.expandedId = this.expandedId === id ? null : id;
    },

    async applyBulkLabel() {
      if (!this.bulkLabel || this.selected.length === 0) return;
      for (const id of this.selected) {
        await fetch("/api/labels", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ account_id: id, label: this.bulkLabel }),
        });
      }
      this.bulkLabel = "";
      this.selected = [];
      await this.loadAccounts();
    },

    // Pipeline
    _initStages() {
      return [
        {
          key: "ingest",
          name: "Ingest Signals",
          status: "pending",
          message: "",
          logs: [],
        },
        {
          key: "score",
          name: "Score Accounts",
          status: "pending",
          message: "",
          logs: [],
        },
        {
          key: "research",
          name: "LLM Research",
          status: "pending",
          message: "",
          logs: [],
        },
        {
          key: "export",
          name: "Export Results",
          status: "pending",
          message: "",
          logs: [],
        },
      ];
    },

    async runPipelineAll() {
      await this._runPipeline([], ["ingest", "score", "research", "export"]);
    },

    async runPipelineSelected() {
      await this._runPipeline(this.selected, [
        "ingest",
        "score",
        "research",
        "export",
      ]);
    },

    async _runPipeline(accountIds, stages) {
      this.pipelineStages = this._initStages();
      this.showPipelineModal = true;
      this.pipelineRunning = true;

      try {
        const resp = await fetch("/api/pipeline/run", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ account_ids: accountIds, stages }),
        });
        const data = await resp.json();
        const runId = data.pipeline_run_id;

        // Connect SSE
        const es = new EventSource(`/api/pipeline/stream/${runId}`);
        es.onmessage = (event) => {
          const evt = JSON.parse(event.data);

          if (evt.type === "stage") {
            const stage = this.pipelineStages.find((s) => s.key === evt.stage);
            if (stage) {
              stage.status = evt.status;
              stage.message = evt.message || "";
            }
          } else if (evt.type === "log") {
            const stage = this.pipelineStages.find((s) => s.key === evt.stage);
            if (stage) {
              stage.logs.push(evt.message);
            }
          } else if (evt.type === "done") {
            es.close();
            this.pipelineRunning = false;
            this.loadAccounts();
          } else if (evt.type === "error") {
            es.close();
            this.pipelineRunning = false;
          }
        };
        es.onerror = () => {
          es.close();
          this.pipelineRunning = false;
        };
      } catch (e) {
        console.error("Pipeline failed:", e);
        this.pipelineRunning = false;
      }
    },
  };
}

function detailPanel() {
  return {
    dtab: "dimensions",
    detail: null,
    researchData: null,
    accountLabels: [],
    newLabel: "",
    newNotes: "",

    // Signal filter inside detail panel
    signalSourceFilter: "",

    // Timeline state
    timelineItems: [],
    timelineTotal: 0,
    timelineOffset: 0,
    timelineSignalCode: "",
    timelineSource: "",

    // Contacts state
    contactsLoading: false,
    enrichingContactId: null,
    contactsSortBy: "actionability",

    get sortedContacts() {
      if (!this.detail || !this.detail.contacts) return [];
      const sorted = [...this.detail.contacts];

      const levelMap = { "C-Level": 5, VP: 4, Director: 3, Manager: 2, IC: 1 };

      sorted.sort((a, b) => {
        if (this.contactsSortBy === "actionability") {
          // Both 0 means they are equal in this sort. If one has any score, it wins.
          const scoreA = (a.authority_score || 0) * (a.warmth_score || 0);
          const scoreB = (b.authority_score || 0) * (b.warmth_score || 0);
          // fallback to auth if actionability is tied
          if (scoreB === scoreA) {
            return (b.authority_score || 0) - (a.authority_score || 0);
          }
          return scoreB - scoreA;
        } else if (this.contactsSortBy === "authority") {
          return (b.authority_score || 0) - (a.authority_score || 0);
        } else if (this.contactsSortBy === "warmth") {
          return (b.warmth_score || 0) - (a.warmth_score || 0);
        } else if (this.contactsSortBy === "level") {
          const lA = levelMap[a.management_level] || 0;
          const lB = levelMap[b.management_level] || 0;
          return lB - lA;
        }
        return 0;
      });
      return sorted;
    },

    async load(accountId) {
      this.researchData = null;
      try {
        const resp = await fetch(`/api/accounts/${accountId}`);
        this.detail = await resp.json();
        this.accountLabels = this.detail.labels || [];
      } catch (e) {
        console.error("Failed to load detail:", e);
      }
    },

    async loadResearch(accountId) {
      this.researchData = null;
      try {
        const resp = await fetch(`/api/research/${accountId}`);
        this.researchData = await resp.json();
      } catch (e) {}
    },

    async loadLabels(accountId) {
      try {
        const resp = await fetch(`/api/labels/${accountId}`);
        const data = await resp.json();
        this.accountLabels = data.labels || [];
      } catch (e) {}
    },

    async loadTimeline(accountId, append) {
      if (!append) {
        this.timelineOffset = 0;
        this.timelineItems = [];
      }
      try {
        const params = new URLSearchParams({
          limit: 50,
          offset: this.timelineOffset,
          signal_code: this.timelineSignalCode,
          source: this.timelineSource,
        });
        const resp = await fetch(
          `/api/accounts/${accountId}/timeline?${params}`,
        );
        const data = await resp.json();
        if (append) {
          this.timelineItems = this.timelineItems.concat(data.items || []);
        } else {
          this.timelineItems = data.items || [];
        }
        this.timelineTotal = data.total || 0;
      } catch (e) {
        console.error("Failed to load timeline:", e);
      }
    },

    async addLabel(accountId) {
      if (!this.newLabel) return;
      await fetch("/api/labels", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          account_id: accountId,
          label: this.newLabel,
          notes: this.newNotes,
        }),
      });
      this.newLabel = "";
      this.newNotes = "";
      await this.loadLabels(accountId);
    },

    async removeLabel(labelId, accountId) {
      await fetch(`/api/labels/${labelId}`, { method: "DELETE" });
      await this.loadLabels(accountId);
    },

    // --- Contacts: Discovery + Enrichment ---

    async discoverContacts(accountId) {
      this.contactsLoading = true;
      try {
        const resp = await fetch(`/api/contacts/${accountId}/discover`, {
          method: "POST",
        });
        const data = await resp.json();
        if (this.detail) {
          this.detail.contacts = data.contacts || [];
        }
      } catch (e) {
        console.error("Failed to discover contacts:", e);
      } finally {
        this.contactsLoading = false;
      }
    },

    async enrichContact(contactId, accountId) {
      this.enrichingContactId = contactId;
      try {
        const resp = await fetch(`/api/contacts/${contactId}/enrich`, {
          method: "POST",
        });
        const data = await resp.json();
        if (data.contact && this.detail && this.detail.contacts) {
          const idx = this.detail.contacts.findIndex(
            (c) => c.contact_id === contactId,
          );
          if (idx !== -1) {
            this.detail.contacts[idx] = data.contact;
          }
        }
      } catch (e) {
        console.error("Failed to enrich contact:", e);
      } finally {
        this.enrichingContactId = null;
      }
    },

    contactStatusColor(status) {
      const map = {
        discovered: "var(--text-muted)",
        ranked: "var(--blue)",
        enriched: "var(--orange)",
        verified: "var(--green)",
      };
      return map[status] || "var(--text-muted)";
    },
  };
}
