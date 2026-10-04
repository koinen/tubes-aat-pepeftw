package model

type VolcanoRef struct {
	VolcanoID string  `json:"volcano_id"`
	Name      string  `json:"name"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}
