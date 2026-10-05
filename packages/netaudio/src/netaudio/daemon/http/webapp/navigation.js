import { computed } from "./lib/preact.js";
import { t } from "./i18n.js";
import { visibleShureDevices } from "./store.js";

export const NAVIGATION = [
  { id: "routing", label: t("Routing"), icon: "routing", path: "/routing", group: "Network" },
  { id: "devices", label: t("Device Info"), icon: "devices", path: "/devices", group: "Network" },
  { id: "clock-status", label: t("Clock Status"), tab: t("Clock"), icon: "clock", path: "/clock-status", group: "Network" },
  { id: "network-status", label: t("Network Status"), tab: t("Network"), icon: "network", path: "/network-status", group: "Network" },
  { id: "events", label: t("Events"), icon: "events", path: "/events", group: "Network" },
  { id: "subscriptions", label: t("Subscriptions"), icon: "subscriptions", path: "/subscriptions", group: "Tools" },
  { id: "presets", label: t("Presets"), icon: "presets", path: "/presets", group: "Tools" },
  { id: "ddm", label: t("Domains"), icon: "ddm", path: "/ddm", group: "Tools" },
  { id: "shure", label: "Shure", icon: "shure", path: "/shure", group: "Tools" },
  { id: "settings", label: t("Settings"), icon: "settings", path: "/settings", group: "Tools" },
];

export const visibleNavigation = computed(() =>
  NAVIGATION.filter((view) => view.id !== "shure" || Object.keys(visibleShureDevices.value).length > 0),
);
