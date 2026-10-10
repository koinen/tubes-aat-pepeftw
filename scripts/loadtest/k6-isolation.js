import http from "k6/http";
import { check } from "k6";
import { bearer } from "./token.js";

const API = __ENV.API_URL || "http://client-api:8080";

export const options = {
	scenarios: {
		bmkg_only: { executor: "constant-vus", vus: 20, duration: "60s", exec: "seismic" },
		pvmbg: { executor: "constant-vus", vus: 20, duration: "60s", exec: "volcanic" },
	},
	thresholds: {
		"http_req_duration{scenario:bmkg_only}": ["p(95)<300"],
		http_req_failed: ["rate<0.01"],
	},
	summaryTrendStats: ["avg", "p(50)", "p(95)", "p(99)", "max"],
};

export function seismic() {
	const res = http.get(`${API}/hazards?type=SEISMIC&limit=20`, {
		headers: { Authorization: `Bearer ${bearer()}` },
		tags: { name: "hazards_seismic" },
	});
	check(res, { "seismic 200": (r) => r.status === 200 });
}

export function volcanic() {
	const res = http.get(`${API}/hazards?type=VOLCANIC&limit=20`, {
		headers: { Authorization: `Bearer ${bearer()}` },
		tags: { name: "hazards_volcanic" },
	});
	check(res, { "volcanic 200": (r) => r.status === 200 });
}
