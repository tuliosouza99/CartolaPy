import { useLocation } from "react-router-dom";
import ThemeToggle from "./ThemeToggle";

const navItems = [
  { path: "/atletas", label: "Atletas" },
  { path: "/pontos-conquistados", label: "Pontos Conquistados" },
  { path: "/pontos-cedidos", label: "Pontos Cedidos" },
  { path: "/confrontos", label: "Confrontos" },
];

function Navbar() {
  const location = useLocation();

  const handleNav = (path) => {
    window.location.href = path;
  };

  return (
    <nav
      style={{
        borderBottom: "1px solid var(--border)",
        background: "var(--bg-card)",
        position: "sticky",
        top: 0,
        zIndex: 100,
        transition:
          "background-color var(--transition), border-color var(--transition)",
      }}
    >
      <div
        style={{
          maxWidth: "1400px",
          margin: "0 auto",
          padding: "1rem 2rem",
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
        }}
      >
        <a
          href="/"
          style={{ display: "flex", alignItems: "center", gap: "0.75rem", textDecoration: "none" }}
        >
          <div
            style={{
              width: "36px",
              height: "36px",
              borderRadius: "var(--radius-sm)",
              background:
                "linear-gradient(135deg, var(--orange) 0%, #EA580C 100%)",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              boxShadow: "0 2px 8px var(--orange-glow)",
            }}
          >
            <svg width="20" height="20" viewBox="0 0 24 24" fill="white">
              <path d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5" />
            </svg>
          </div>
          <span
            style={{
              fontFamily: "var(--font-display)",
              fontSize: "1.25rem",
              fontWeight: 700,
              color: "var(--text-primary)",
              letterSpacing: "-0.02em",
            }}
          >
            Cartola<span style={{ color: "var(--orange)" }}>Py</span>
          </span>
        </a>

        <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
          {navItems.map((item) => (
            <button
              key={item.path}
              onClick={() => handleNav(item.path)}
              style={{
                fontFamily: "var(--font-display)",
                fontWeight: 500,
                fontSize: "0.875rem",
                padding: "0.5rem 1rem",
                borderRadius: "var(--radius-sm)",
                border: "none",
                cursor: "pointer",
                color:
                  location.pathname === item.path
                    ? "var(--orange)"
                    : "var(--text-secondary)",
                background:
                  location.pathname === item.path
                    ? "rgba(249, 115, 22, 0.1)"
                    : "transparent",
                transition: "all var(--transition)",
              }}
              onMouseEnter={(e) => {
                if (location.pathname !== item.path) {
                  e.target.style.color = "var(--text-primary)";
                  e.target.style.background = "var(--bg-tertiary)";
                }
              }}
              onMouseLeave={(e) => {
                if (location.pathname !== item.path) {
                  e.target.style.color = "var(--text-secondary)";
                  e.target.style.background = "transparent";
                }
              }}
            >
              {item.label}
            </button>
          ))}
        </div>

        <ThemeToggle />
      </div>
    </nav>
  );
}

export default Navbar;
