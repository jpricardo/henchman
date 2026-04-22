package budget

import "testing"

func TestGlobalBudget_SuccessfulReservation(t *testing.T) {
	gb := NewGlobalBudget(128)

	err := gb.Reserve(16)
	if err != nil {
		t.Error(err.Error())
	}
}

func TestGlobalBudget_OverBudgetRejection(t *testing.T) {
	gb := NewGlobalBudget(128)

	err := gb.Reserve(129)
	if err == nil {
		t.Error("Expected over budget reservation to fail")
	}
}

func TestGlobalBudget_ConcurrentReservations(t *testing.T) {
	gb := NewGlobalBudget(128)

	ch := make(chan error)

	for range 10 {
		go func(c chan error) {
			c <- gb.Reserve(32)
		}(ch)
	}

	for range 10 {
		<-ch
	}

	if gb.allocatedBytes > gb.totalBytes {
		t.Errorf("Allocated size %d exceeds max size %d", gb.allocatedBytes, gb.totalBytes)
	}
}
