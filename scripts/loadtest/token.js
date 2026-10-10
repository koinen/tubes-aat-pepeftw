import http from "k6/http";

const AUTH = __ENV.AUTH_URL || "http://auth:8080";

let token = "";
let expiresAt = 0;

export function bearer() {
	if (Date.now() < expiresAt) {
		return token;
	}
	const res = http.post(
		`${AUTH}/token`,
		JSON.stringify({ grant_type: "client_credentials", client_id: "bnpb-ops", client_secret: __ENV.BNPB_SECRET }),
		{ headers: { "Content-Type": "application/json" }, tags: { name: "token" } },
	);
	token = res.json("access_token");
	expiresAt = Date.now() + (res.json("expires_in") - 10) * 1000;
	return token;
}
