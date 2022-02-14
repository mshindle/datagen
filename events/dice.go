package events

import "encoding/json"

type Die struct {
	Sided int
}

// Roll returns the simulated result of rolling the die
func (d Die) Roll() int {
	return random.Intn(d.Sided) + 1
}

type ThrowResult struct {
	Total int   `json:"total"`
	Rolls []int `json:"rolls"`
}

func (tr ThrowResult) Serialize() ([]byte, error) {
	return json.Marshal(tr)
}

type Cup struct {
	dice []Die
}

func (c Cup) Throw() ThrowResult {
	var total int
	rolls := make([]int, len(c.dice))
	for i, d := range c.dice {
		rolls[i] = d.Roll()
		total += rolls[i]
	}
	return ThrowResult{Total: total, Rolls: rolls}
}

// NewCup creates a cupful of dice with the number of sides
// equal to sided, and the number of dice equal to amt.
func NewCup(sided, amt int) *Cup {
	dice := make([]Die, amt)
	for i := 0; i < amt; i++ {
		dice[i] = Die{Sided: sided}
	}
	return &Cup{dice: dice}
}
