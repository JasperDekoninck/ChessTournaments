const initTeamRegistration = () => {
  document.querySelectorAll("[data-team-registration]").forEach((form) => {
    if (form.dataset.boundTeamRegistration) return;
    form.dataset.boundTeamRegistration = "1";
    const mode = form.querySelector("[data-registration-mode]");
    const update = () => {
      const solo = mode.value === "solo";
      [["[data-team-fields]", solo], ["[data-solo-fields]", !solo]].forEach(([selector, hidden]) => {
        const fields = form.querySelector(selector);
        fields.hidden = hidden;
        fields.querySelectorAll("input, textarea").forEach((input) => { input.disabled = hidden; });
      });
    };
    mode.addEventListener("change", update);
    form.addEventListener("change", update);
    update();
  });

  document.querySelectorAll("[data-tournament-format]").forEach((form) => {
    if (form.dataset.boundTournamentFormat) return;
    form.dataset.boundTournamentFormat = "1";
    const team = form.querySelector('[name="is_team"]');
    const excluded = form.querySelector('[name="excludes_rating"]');
    const size = form.querySelector('[data-team-size]');
    const update = () => {
      if (team.checked) excluded.checked = true;
      excluded.disabled = team.checked;
      size.hidden = !team.checked;
      size.querySelector("input").disabled = !team.checked;
    };
    team.addEventListener("change", update);
    update();
  });

  document.querySelectorAll("[data-team-assignment]").forEach((form) => {
    if (form.dataset.boundTeamAssignment) return;
    form.dataset.boundTeamAssignment = "1";
    const team = form.querySelector('[name="existing_entry_id"]');
    const name = form.querySelector('[name="team_name"]');
    const update = () => {
      name.disabled = !!team.value;
      name.required = !team.value;
    };
    team.addEventListener("change", update);
    update();
  });
};
document.addEventListener("DOMContentLoaded", initTeamRegistration);
document.addEventListener("admin:entries-updated", initTeamRegistration);
