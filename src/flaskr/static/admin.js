const ready = () => {
  initConfirmForms();
  initOpenRoundPreservers();
  initRoundButtons();
  initRoundEditors();
  initRegistrationFieldEditor();
  initMemberDirectorySearch();
  initMemberDirectoryPagination();
  initMemberStatusToggles();
  initPlayerSearch();
  initPlayerSorting();
  initEntryToggles();
  initAvailabilityToggles();
  initModals();
};

const initConfirmForms = () => {
  document.querySelectorAll("form[data-confirm]").forEach((form) => {
    form.addEventListener("submit", (event) => {
      if (!window.confirm(form.dataset.confirm || "Are you sure?")) {
        event.preventDefault();
      }
    });
  });
};

const currentOpenRound = () => document.querySelector("[data-round-open].is-active")?.dataset.roundOpen || "";

const initOpenRoundPreservers = () => {
  document.querySelectorAll("form[data-preserve-open-round]").forEach((form) => {
    if (form.dataset.boundOpenRound === "1") {
      return;
    }
    form.dataset.boundOpenRound = "1";
    form.addEventListener("submit", () => {
      const input = form.querySelector('input[name="open_round"]');
      if (input) {
        input.value = currentOpenRound();
      }
    });
  });
};

const openRoundPanel = (roundNo) => {
  document.querySelectorAll("[data-round-panel]").forEach((panel) => {
    panel.hidden = panel.id !== `round-${roundNo}`;
  });
  document.querySelectorAll("[data-round-open]").forEach((button) => {
    button.classList.toggle("is-active", button.dataset.roundOpen === String(roundNo));
  });
};

const initRoundButtons = () => {
  const initialNextRound = document
    .querySelector("[data-generate-form]:not([hidden])")
    ?.closest("[data-round-panel]")
    ?.dataset.roundPanelNo;
  updateGenerateForms(initialNextRound ? Number(initialNextRound) : null);
  document.querySelectorAll("[data-round-open]").forEach((button) => {
    button.addEventListener("click", () => {
      if (button.classList.contains("is-active")) {
        document.querySelectorAll("[data-round-panel]").forEach((panel) => {
          panel.hidden = true;
        });
        document.querySelectorAll("[data-round-open]").forEach((item) => {
          item.classList.remove("is-active");
        });
        return;
      }
      openRoundPanel(button.dataset.roundOpen);
    });
  });
};

const setSaveStatus = (form, message, isError = false) => {
  const node = form.querySelector("[data-save-status]");
  if (!node) {
    return;
  }
  if (!message) {
    node.hidden = true;
    return;
  }
  node.hidden = false;
  node.textContent = message;
  node.classList.toggle("save-error", isError);
};

const updatePairingDisplay = (row) => {
  const whiteSelect = row.querySelector('select[name^="white_"]');
  const blackSelect = row.querySelector('select[name^="black_"]');
  const resultSelect = row.querySelector('select[name^="result_"]');
  if (resultSelect) {
    const byeOption = Array.from(resultSelect.options).find((option) => option.value === "BYE");
    const hasOpponent = Boolean(blackSelect?.value);
    if (byeOption) {
      byeOption.disabled = hasOpponent;
    }
    if (hasOpponent && resultSelect.value === "BYE") {
      resultSelect.value = "";
    }
  }
  const displays = row.querySelectorAll(".pairing-display");
  if (whiteSelect && displays[0]) {
    displays[0].textContent = whiteSelect.options[whiteSelect.selectedIndex]?.text || "Choose white";
  }
  if (resultSelect && displays[1]) {
    displays[1].textContent = resultSelect.value || "—";
  }
  if (blackSelect && displays[2]) {
    displays[2].textContent = blackSelect.options[blackSelect.selectedIndex]?.text || "Bye / empty";
  }
  row.classList.toggle("is-empty", !whiteSelect?.value);
};

const formatStandingValue = (value) => {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric.toFixed(1) : "0.0";
};

const updateGenerateForms = (nextRound) => {
  document.querySelectorAll("[data-round-panel]").forEach((panel) => {
    const form = panel.querySelector("[data-generate-form]");
    if (!form) {
      return;
    }
    const panelRound = Number(panel.dataset.roundPanelNo || 0);
    const hasPairings = panel.dataset.hasPairings === "1";
    form.hidden = !(nextRound && panelRound === Number(nextRound) && !hasPairings);
  });
};

let applyActivePlayerSort = () => {};

const bindAvailabilityToggle = (form) => {
  if (!form || form.dataset.boundAvailability === "1") {
    return;
  }
  form.dataset.boundAvailability = "1";
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const response = await fetch(form.action, {
      method: "POST",
      headers: {
        "X-CSRF-Token": form.querySelector('input[name="csrf_token"]')?.value || "",
        "X-Requested-With": "XMLHttpRequest",
      },
      body: new FormData(form),
    });
    const payload = await response.json();
    if (!response.ok || !payload.ok) {
      return;
    }
    if (payload.entry) {
      applyEntryState(payload.entry);
    }
  });
};

const renderRoundCellContent = (row, roundNo, roundInfo) => {
  const cellNode = row.querySelector(`[data-round-cell="${roundNo}"]`);
  if (!cellNode) {
    return;
  }
  const cell = roundInfo.cell || roundInfo;
  cellNode.className = `round-cell round-cell-${cell.kind}`;
  if (roundInfo.can_toggle) {
    const csrfToken = row.dataset.csrfToken || "";
    const action = row.dataset.availabilityUrl || "";
    cellNode.innerHTML = `
      <form method="post" action="${action}" data-toggle-availability>
        <input type="hidden" name="csrf_token" value="${csrfToken}">
        <input type="hidden" name="round_no" value="${roundNo}">
        <button type="submit" class="tiny secondary" data-availability-button>${cell.kind === "out" || cell.kind === "future-out" ? "out" : "in"}</button>
      </form>
    `;
    bindAvailabilityToggle(cellNode.querySelector("form[data-toggle-availability]"));
    return;
  }
  cellNode.textContent = cell.label || "";
};

const renderEntryRoundCells = (row, entry) => {
  (entry.round_cells || []).forEach((roundInfo) => {
    renderRoundCellContent(row, roundInfo.round_no, roundInfo);
  });
};

const applyRoundCell = (row, roundNo, cell) => {
  renderRoundCellContent(row, roundNo, { round_no: roundNo, cell, can_toggle: false });
};

const applyEntryUpdates = (roundNo, entryUpdates) => {
  (entryUpdates || []).forEach((entry) => {
    const row = document.querySelector(`tr[data-entry-id="${entry.id}"]`);
    if (!row) {
      return;
    }
    const scoreCell = row.querySelector("[data-score-cell]");
    const bhCell = row.querySelector("[data-bh-cell]");
    const bhc1Cell = row.querySelector("[data-bhc1-cell]");
    if (scoreCell) {
      scoreCell.textContent = formatStandingValue(entry.score);
    }
    if (bhCell) {
      bhCell.textContent = formatStandingValue(entry.bh);
    }
    if (bhc1Cell) {
      bhc1Cell.textContent = formatStandingValue(entry.bh_c1);
    }
    row.dataset.score = formatStandingValue(entry.score);
    row.dataset.bh = formatStandingValue(entry.bh);
    row.dataset.bhc1 = formatStandingValue(entry.bh_c1);
    if (entry.round_cell) {
      applyRoundCell(row, roundNo, entry.round_cell);
    }
  });
  applyActivePlayerSort();
};

const bindPairingRow = (form, row, openRow) => {
  updatePairingDisplay(row);
  row.addEventListener("click", (event) => {
    if (event.target.closest("select")) {
      return;
    }
    openRow(row);
  });
  row.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      row.classList.remove("is-editing");
      return;
    }
    if (event.key !== "Enter" && event.key !== " ") {
      return;
    }
    event.preventDefault();
    openRow(row);
  });
  row.querySelectorAll("[data-pairing-field]").forEach((field) => {
    field.addEventListener("click", (event) => {
      if (event.target.closest("select")) {
        return;
      }
      event.stopPropagation();
      openRow(row);
      const select = field.querySelector("select");
      if (!select) {
        return;
      }
      window.requestAnimationFrame(() => {
        select.focus();
        if (typeof select.showPicker === "function") {
          select.showPicker();
        } else {
          select.click();
        }
      });
    });
  });
};

const initRoundEditors = () => {
  document.querySelectorAll(".autosave-round-form").forEach((form) => {
    let timer = null;
    const renumberRows = () => {
      const rows = Array.from(form.querySelectorAll("[data-pairing-row]"));
      rows.forEach((row, index) => {
        const boardNo = index + 1;
        const label = row.querySelector("[data-board-label]");
        if (label) {
          label.textContent = `Board ${boardNo}`;
        }
        row.querySelectorAll('select[name^="white_"]').forEach((select) => {
          select.name = `white_${boardNo}`;
        });
        row.querySelectorAll('select[name^="result_"]').forEach((select) => {
          select.name = `result_${boardNo}`;
        });
        row.querySelectorAll('select[name^="black_"]').forEach((select) => {
          select.name = `black_${boardNo}`;
        });
      });
      const countInput = form.querySelector("[data-board-count]");
      if (countInput) {
        countInput.value = String(rows.length);
      }
    };
    const closeRows = () => {
      form.querySelectorAll("[data-pairing-row].is-editing").forEach((row) => {
        row.classList.remove("is-editing");
      });
    };
    const openRow = (row) => {
      closeRows();
      row.classList.add("is-editing");
    };

    const addBoard = () => {
      const list = form.querySelector("[data-pairing-list]");
      const template = form.querySelector("[data-empty-board-template]");
      const countInput = form.querySelector("[data-board-count]");
      if (!list || !template || !countInput) {
        return;
      }
      const nextBoardNo = form.querySelectorAll("[data-pairing-row]").length + 1;
      const wrapper = document.createElement("div");
      wrapper.innerHTML = template.innerHTML.replaceAll("__BOARD__", String(nextBoardNo)).trim();
      const row = wrapper.firstElementChild;
      if (!row) {
        return;
      }
      list.appendChild(row);
      renumberRows();
      bindPairingRow(form, row, openRow);
      openRow(row);
    };

    const save = async () => {
      try {
        const response = await fetch(form.action, {
          method: "POST",
          headers: {
            "X-CSRF-Token": form.querySelector('input[name="csrf_token"]')?.value || "",
            "X-Requested-With": "XMLHttpRequest",
          },
          body: new FormData(form),
        });
        const payload = await response.json();
        if (!response.ok || !payload.ok) {
          throw new Error(payload.message || "Save failed.");
        }
        setSaveStatus(form, "");
        applyEntryUpdates(Number(form.dataset.roundNo), payload.entry_updates);
        updateGenerateForms(payload.next_round);
      } catch (error) {
        setSaveStatus(form, error.message || "Save failed.", true);
      }
    };

    form.querySelectorAll("[data-pairing-row]").forEach((row) => {
      bindPairingRow(form, row, openRow);
    });

    const addBoardButton = form.closest("[data-round-panel]")?.querySelector("[data-add-board]");
    if (addBoardButton) {
      addBoardButton.addEventListener("click", addBoard);
    }

    form.addEventListener("change", (event) => {
      const row = event.target.closest("[data-pairing-row]");
      if (row) {
        updatePairingDisplay(row);
      }
      window.clearTimeout(timer);
      timer = window.setTimeout(save, 180);
    });

    document.addEventListener("click", (event) => {
      if (!form.contains(event.target) || !event.target.closest("[data-pairing-row]")) {
        closeRows();
      }
    });
  });
};

const replaceMemberDirectoryRoot = (html, restoreSearch = null) => {
  const parser = new DOMParser();
  const doc = parser.parseFromString(html, "text/html");
  const nextRoot = doc.querySelector("[data-member-directory-root]");
  const currentRoot = document.querySelector("[data-member-directory-root]");
  if (!nextRoot || !currentRoot) {
    return;
  }
  currentRoot.replaceWith(nextRoot);
  initMemberDirectorySearch();
  initMemberDirectoryPagination();
  initMemberStatusToggles();
  if (restoreSearch) {
    const input = document.querySelector('form[data-member-search] input[name="member_q"]');
    if (input) {
      input.value = restoreSearch.value;
      input.focus();
      if (typeof input.setSelectionRange === "function") {
        input.setSelectionRange(restoreSearch.start, restoreSearch.end);
      }
    }
  }
};

const loadMemberDirectory = async (url, options = {}) => {
  const { restoreSearch = null, ...fetchOptions } = options;
  const response = await fetch(url, {
    ...fetchOptions,
    headers: {
      "X-Requested-With": "XMLHttpRequest",
      ...(fetchOptions.headers || {}),
    },
  });
  const html = await response.text();
  if (!response.ok) {
    throw new Error("Could not update the member directory.");
  }
  replaceMemberDirectoryRoot(html, restoreSearch);
};

const initMemberDirectorySearch = () => {
  document.querySelectorAll("form[data-member-search]").forEach((form) => {
    if (form.dataset.boundMemberSearch === "1") {
      return;
    }
    form.dataset.boundMemberSearch = "1";
    const input = form.querySelector('input[name="member_q"]');
    if (!input) {
      return;
    }
    let timer = null;
    let controller = null;
    let lastValue = input.value;

    const submitSearch = async () => {
      const params = new URLSearchParams(new FormData(form));
      controller?.abort();
      controller = new AbortController();
      try {
        await loadMemberDirectory(`${form.action}?${params.toString()}`, {
          signal: controller.signal,
          restoreSearch: {
            value: input.value,
            start: input.selectionStart ?? input.value.length,
            end: input.selectionEnd ?? input.value.length,
          },
        });
      } catch (error) {
        if (error.name !== "AbortError") {
          return;
        }
      }
    };

    form.addEventListener("submit", (event) => {
      event.preventDefault();
      submitSearch();
    });

    input.addEventListener("input", () => {
      window.clearTimeout(timer);
      timer = window.setTimeout(() => {
        if (input.value === lastValue) {
          return;
        }
        lastValue = input.value;
        submitSearch();
      }, 320);
    });
  });
};

const initMemberDirectoryPagination = () => {
  document.querySelectorAll("[data-member-directory-root] .pagination-link").forEach((link) => {
    if (link.dataset.boundMemberPagination === "1") {
      return;
    }
    link.dataset.boundMemberPagination = "1";
    link.addEventListener("click", async (event) => {
      event.preventDefault();
      await loadMemberDirectory(link.href);
    });
  });
};

const initMemberStatusToggles = () => {
  document.querySelectorAll("form[data-member-status-form]").forEach((form) => {
    if (form.dataset.boundMemberStatus === "1") {
      return;
    }
    form.dataset.boundMemberStatus = "1";
    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      const response = await fetch(form.action, {
        method: "POST",
        headers: {
          "X-CSRF-Token": form.querySelector('input[name="csrf_token"]')?.value || "",
          "X-Requested-With": "XMLHttpRequest",
        },
        body: new FormData(form),
      });
      const payload = await response.json();
      if (!response.ok || !payload.ok) {
        return;
      }
      const button = form.querySelector("[data-member-status-button]");
      const nextValue = form.querySelector("[data-member-next-value]");
      if (button) {
        button.textContent = payload.label;
        button.classList.remove("status-on", "status-off");
        button.classList.add(payload.is_member ? "status-on" : "status-off");
      }
      if (nextValue) {
        nextValue.value = payload.is_member ? "0" : "1";
      }
    });
  });
};

const initPlayerSearch = () => {
  document.querySelectorAll("[data-player-search]").forEach((form) => {
    const nameInput = form.querySelector("[data-player-name]");
    const emailInput = form.querySelector("[data-player-email]");
    const ratingInput = form.querySelector("[data-player-rating]");
    const results = form.querySelector("[data-player-results]");
    let controller = null;

    const hideResults = () => {
      results.hidden = true;
      results.innerHTML = "";
    };

    const selectPlayer = (item) => {
      nameInput.value = item.name;
      if (!emailInput.value && item.email) {
        emailInput.value = item.email;
      }
      if (item.member && item.rating !== null && item.rating !== undefined) {
        ratingInput.value = item.rating;
      }
      hideResults();
    };

    nameInput.addEventListener("input", async () => {
      const term = nameInput.value.trim();
      if (term.length < 2) {
        hideResults();
        return;
      }
      controller?.abort();
      controller = new AbortController();
      try {
        const response = await fetch(`/admin/player-suggestions?q=${encodeURIComponent(term)}`, {
          signal: controller.signal,
        });
        const payload = await response.json();
        const items = payload.items || [];
        if (!items.length) {
          hideResults();
          return;
        }
        results.innerHTML = "";
        items.forEach((item) => {
          const button = document.createElement("button");
          button.type = "button";
          button.className = "autocomplete-option";
          button.textContent = `${item.name}${item.member ? " · member" : ""}${item.rating ? ` · ${item.rating}` : ""}`;
          button.addEventListener("click", () => selectPlayer(item));
          results.appendChild(button);
        });
        results.hidden = false;
      } catch (error) {
        if (error.name !== "AbortError") {
          hideResults();
        }
      }
    });

    document.addEventListener("click", (event) => {
      if (!form.contains(event.target)) {
        hideResults();
      }
    });
  });
};

const initPlayerSorting = () => {
  const button = document.querySelector("[data-player-sort]");
  const scoreButton = document.querySelector("[data-player-sort-score]");
  const tableBody = document.querySelector("[data-player-table-body]");
  if (!button || !scoreButton || !tableBody) {
    return;
  }

  const rows = () => Array.from(tableBody.querySelectorAll("tr[data-entry-id]"));

  const render = (sortedRows) => {
    sortedRows.forEach((row) => tableBody.appendChild(row));
  };

  const setIndicators = (kind, direction) => {
    button.textContent = kind === "name" ? (direction === "desc" ? "↓" : "↑") : "↕";
    scoreButton.textContent = kind === "score" ? (direction === "asc" ? "↑" : "↓") : "↕";
    tableBody.dataset.sortKind = kind;
    tableBody.dataset.sortDirection = direction;
  };

  const sortByDefault = () => {
    render(
      rows().sort(
        (left, right) => Number(left.dataset.defaultOrder || 0) - Number(right.dataset.defaultOrder || 0)
      )
    );
    setIndicators("default", "default");
  };

  const sortByName = (direction) => {
    const multiplier = direction === "desc" ? -1 : 1;
    render(
      rows().sort((left, right) => {
        const nameCompare = (left.dataset.playerName || "").localeCompare(right.dataset.playerName || "");
        if (nameCompare !== 0) {
          return nameCompare * multiplier;
        }
        return Number(left.dataset.defaultOrder || 0) - Number(right.dataset.defaultOrder || 0);
      })
    );
    setIndicators("name", direction);
  };

  const sortByScore = (direction) => {
    const multiplier = direction === "asc" ? 1 : -1;
    render(
      rows().sort((left, right) => {
        const scoreCompare = (Number(left.dataset.score || 0) - Number(right.dataset.score || 0)) * multiplier;
        if (scoreCompare !== 0) {
          return scoreCompare;
        }
        const bhCompare = (Number(left.dataset.bh || 0) - Number(right.dataset.bh || 0)) * multiplier;
        if (bhCompare !== 0) {
          return bhCompare;
        }
        const bhc1Compare = (Number(left.dataset.bhc1 || 0) - Number(right.dataset.bhc1 || 0)) * multiplier;
        if (bhc1Compare !== 0) {
          return bhc1Compare;
        }
        return (left.dataset.playerName || "").localeCompare(right.dataset.playerName || "");
      })
    );
    setIndicators("score", direction);
  };

  applyActivePlayerSort = () => {
    const kind = tableBody.dataset.sortKind || "default";
    const direction = tableBody.dataset.sortDirection || "default";
    if (kind === "name") {
      sortByName(direction);
      return;
    }
    if (kind === "score") {
      sortByScore(direction);
      return;
    }
    sortByDefault();
  };

  button.addEventListener("click", () => {
    const nextDirection =
      tableBody.dataset.sortKind === "name" && tableBody.dataset.sortDirection === "asc" ? "desc" : "asc";
    sortByName(nextDirection);
  });

  button.addEventListener("dblclick", (event) => {
    event.preventDefault();
    sortByDefault();
  });

  scoreButton.addEventListener("click", () => {
    const nextDirection =
      tableBody.dataset.sortKind === "score" && tableBody.dataset.sortDirection === "desc" ? "asc" : "desc";
    sortByScore(nextDirection);
  });

  scoreButton.addEventListener("dblclick", (event) => {
    event.preventDefault();
    sortByDefault();
  });

  sortByDefault();
};

const applyEntryState = (entry) => {
  const row = document.querySelector(`[data-entry-id="${entry.id}"]`);
  if (!row) {
    return;
  }
  row.classList.toggle("is-muted", !entry.is_active);
  row.classList.toggle("is-waitlist", entry.waitlist_position !== null);
  const button = row.querySelector("[data-entry-status]");
  if (button) {
    button.textContent = entry.label;
    button.classList.remove("status-on", "status-off", "status-waitlist");
    button.classList.add(
      entry.state === "waitlist" ? "status-waitlist" : entry.state === "active" ? "status-on" : "status-off"
    );
  }
  const note = row.querySelector(".waitlist-note");
  if (entry.waitlist_position === null) {
    if (note) {
      note.remove();
    }
  } else if (note) {
    note.textContent = `Waiting list #${entry.waitlist_position}`;
  }
  if (entry.round_cells) {
    renderEntryRoundCells(row, entry);
  }
  if (entry.next_round !== undefined) {
    updateGenerateForms(entry.next_round);
  }
};

const initEntryToggles = () => {
  document.querySelectorAll("form[data-toggle-entry]").forEach((form) => {
    if (form.dataset.boundToggleEntry === "1") {
      return;
    }
    form.dataset.boundToggleEntry = "1";
    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      const response = await fetch(form.action, {
        method: "POST",
        headers: {
          "X-CSRF-Token": form.querySelector('input[name="csrf_token"]')?.value || "",
          "X-Requested-With": "XMLHttpRequest",
        },
        body: new FormData(form),
      });
      const payload = await response.json();
      if (!response.ok || !payload.ok) {
        return;
      }
      if (payload.entry) {
        applyEntryState(payload.entry);
      }
      (payload.waitlist || []).forEach(applyEntryState);
    });
  });
};

const initAvailabilityToggles = () => {
  document.querySelectorAll("form[data-toggle-availability]").forEach((form) => {
    bindAvailabilityToggle(form);
  });
};

const setModalState = (name, open) => {
  const modal = document.querySelector(`[data-modal="${name}"]`);
  if (!modal) {
    return;
  }
  modal.hidden = !open;
  document.body.classList.toggle("modal-open", open);
};

const initModals = () => {
  document.querySelectorAll("[data-open-modal]").forEach((button) => {
    button.addEventListener("click", () => setModalState(button.dataset.openModal, true));
  });
  document.querySelectorAll("[data-close-modal]").forEach((button) => {
    button.addEventListener("click", () => setModalState(button.dataset.closeModal, false));
  });
  document.querySelectorAll("[data-modal]").forEach((modal) => {
    modal.addEventListener("click", (event) => {
      if (event.target === modal) {
        setModalState(modal.dataset.modal, false);
      }
    });
  });
};

const initRegistrationFieldEditor = () => {
  const container = document.querySelector("[data-registration-fields]");
  if (!container) {
    return;
  }
  const emptyState = document.querySelector("[data-registration-fields-empty]");
  const templates = {
    text: document.querySelector('[data-registration-field-template="text"]'),
    dropdown: document.querySelector('[data-registration-field-template="dropdown"]'),
  };

  const syncEmptyState = () => {
    if (emptyState) {
      emptyState.hidden = container.querySelector("[data-registration-field]") !== null;
    }
  };

  const bindField = (field) => {
    const removeButton = field.querySelector("[data-remove-registration-field]");
    if (removeButton) {
      removeButton.addEventListener("click", () => {
        field.remove();
        syncEmptyState();
      });
    }
  };

  container.querySelectorAll("[data-registration-field]").forEach(bindField);

  document.querySelectorAll("[data-add-registration-field]").forEach((button) => {
    button.addEventListener("click", () => {
      const template = templates[button.dataset.addRegistrationField];
      if (!template) {
        return;
      }
      const wrapper = document.createElement("div");
      wrapper.innerHTML = template.innerHTML.trim();
      const field = wrapper.firstElementChild;
      if (!field) {
        return;
      }
      container.appendChild(field);
      bindField(field);
      syncEmptyState();
      field.querySelector('input[name="registration_field_label"]')?.focus();
    });
  });

  syncEmptyState();
};

document.addEventListener("DOMContentLoaded", ready);
