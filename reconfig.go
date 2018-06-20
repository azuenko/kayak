package kayak

func (k *Kayak) addProcess(t Tracer, processKey KAddress) {
	t = t.Fork("addProcess")
	k.traceF(t.Logf("%#v", processKey))

	if _, exists := k.rkeys[processKey]; exists {
		k.traceF(t.Logf("process already exists, abort"))
		return
	}

	leaderPos := uint(k.epoch) % k.n

	epochDelta := KEpoch(0)
	for {
		if uint(k.epoch+epochDelta)%(k.n+1) == leaderPos {
			break
		}
		epochDelta++
	}

	if epochDelta > 0 {
		k.traceF(t.Logf("epoch jump from %#v to %#v to keep current leader %#v at pos %d",
			k.epoch, k.epoch+epochDelta, k.leader(), leaderPos))
		k.epoch += epochDelta
	} else {
		k.traceF(t.Logf("no epoch jump needed"))
	}

	k.traceF(t.Logf("increasing keys size from %d to %d", len(k.keys), len(k.keys)+1))
	k.keys = append(k.keys, processKey)
	k.rkeys[processKey] = len(k.keys) - 1
	k.updateFactors()
	k.localClient.ReconfigureTo(k.keys)

}

func (k *Kayak) removeProcess(t Tracer, processKey KAddress) {
	t = t.Fork("removeProcess")

	if _, exists := k.rkeys[processKey]; !exists {
		k.traceF(t.Logf("process does not exist, abort"))
		return
	}

	if k.n == 1 {
		k.traceF(t.Logf("at least one process should remain, abort"))
		return
	}

	k.traceF(t.Logf("removing process %#v", processKey))

	if processKey == k.leader() {
		k.traceF(t.Logf("removing current leader, no epoch jump"))
	} else {
		k.traceF(t.Logf("removing not leader, performing epoch jump"))
		leaderPos := uint(k.epoch) % k.n

		epochDelta := KEpoch(0)
		for {
			if uint(k.epoch+epochDelta)%(k.n-1) == leaderPos {
				break
			}
			epochDelta++
		}

		if epochDelta > 0 {
			k.traceF(t.Logf("epoch jump from %#v to %#v to keep current leader %#v at pos %d",
				k.epoch, k.epoch+epochDelta, k.leader(), leaderPos))
			k.epoch += epochDelta
		} else {
			k.traceF(t.Logf("no epoch jump needed"))
		}
	}

	processPos := k.rkeys[processKey]

	k.traceF(t.Logf("decreasing keys size from %d to %d", len(k.keys), len(k.keys)-1))
	k.keys = append(k.keys[:processPos], k.keys[processPos+1:]...)
	delete(k.rkeys, processKey)

	k.updateFactors()
	k.localClient.ReconfigureTo(k.keys)

	proposes := make(map[KRound]map[KEpoch]map[KAddress]KPropose)
	for round := range k.proposes {
		if _, found := proposes[round]; !found {
			proposes[round] = make(map[KEpoch]map[KAddress]KPropose)
		}
		for epoch := range k.proposes[round] {
			if _, found := proposes[round][epoch]; !found {
				proposes[round][epoch] = make(map[KAddress]KPropose)
			}
			for address := range k.proposes[round][epoch] {
				if address != processKey {
					proposes[round][epoch][address] = k.proposes[round][epoch][address]
				}
			}
		}
	}
	k.proposes = proposes

	writes := make(map[KRound]map[KEpoch]map[KHash]map[KAddress]struct{})
	for round := range k.writes {
		if _, found := writes[round]; !found {
			writes[round] = make(map[KEpoch]map[KHash]map[KAddress]struct{})
		}
		for epoch := range k.writes[round] {
			if _, found := writes[round][epoch]; !found {
				writes[round][epoch] = make(map[KHash]map[KAddress]struct{})
			}
			for hash := range k.writes[round][epoch] {
				if _, found := writes[round][epoch][hash]; !found {
					writes[round][epoch][hash] = make(map[KAddress]struct{})
				}
				for address := range k.writes[round][epoch][hash] {
					if address != processKey {
						writes[round][epoch][hash][address] = k.writes[round][epoch][hash][address]
					}
				}
			}
		}
	}
	k.writes = writes

	accepts := make(map[KRound]map[KEpoch]map[KHash]map[KAddress]struct{})
	for round := range k.accepts {
		if _, found := accepts[round]; !found {
			accepts[round] = make(map[KEpoch]map[KHash]map[KAddress]struct{})
		}
		for epoch := range k.accepts[round] {
			if _, found := accepts[round][epoch]; !found {
				accepts[round][epoch] = make(map[KHash]map[KAddress]struct{})
			}
			for hash := range k.accepts[round][epoch] {
				if _, found := accepts[round][epoch][hash]; !found {
					accepts[round][epoch][hash] = make(map[KAddress]struct{})
				}
				for address := range k.accepts[round][epoch][hash] {
					if address != processKey {
						accepts[round][epoch][hash][address] = k.accepts[round][epoch][hash][address]
					}
				}
			}
		}
	}
	k.accepts = accepts

	suspects := make(map[KEpoch]map[KAddress]KSuspect)
	for epoch := range k.suspects {
		if _, found := suspects[epoch]; !found {
			suspects[epoch] = make(map[KAddress]KSuspect)
		}
		for address := range k.suspects[epoch] {
			if address != processKey {
				suspects[epoch][address] = k.suspects[epoch][address]
			}
		}
	}
	k.suspects = suspects

	heads := make(map[KRound]map[KEpoch]map[KAddress]struct{})
	for round := range k.heads {
		if _, found := heads[round]; !found {
			heads[round] = make(map[KEpoch]map[KAddress]struct{})
		}
		for epoch := range k.heads[round] {
			if _, found := heads[round][epoch]; !found {
				heads[round][epoch] = make(map[KAddress]struct{})
			}
			for address := range k.heads[round][epoch] {
				if address != processKey {
					heads[round][epoch][address] = k.heads[round][epoch][address]
				}
			}
		}
	}
	k.heads = heads

	confirms := make(map[KRound]map[KHash]map[KHash]map[KAddress]struct{})
	for round := range k.confirms {
		if _, found := confirms[round]; !found {
			confirms[round] = make(map[KHash]map[KHash]map[KAddress]struct{})
		}
		for dataHash := range k.confirms[round] {
			if _, found := confirms[round][dataHash]; !found {
				confirms[round][dataHash] = make(map[KHash]map[KAddress]struct{})
			}
			for buzzHash := range k.confirms[round][dataHash] {
				if _, found := confirms[round][dataHash][buzzHash]; !found {
					confirms[round][dataHash][buzzHash] = make(map[KAddress]struct{})
				}
				for address := range k.confirms[round][dataHash][buzzHash] {
					if address != processKey {
						confirms[round][dataHash][buzzHash][address] = k.confirms[round][dataHash][buzzHash][address]
					}
				}
			}
		}
	}
	k.confirms = confirms

}
