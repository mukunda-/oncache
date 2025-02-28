//////////////////////////////////////////////////////////////////////////////////////////
// oncache (C) 2025 Mukunda Johnson (mukunda.com)
// Licensed under MIT. See LICENSE file.
//////////////////////////////////////////////////////////////////////////////////////////

package mapchain

type FilterPredicate = func(value any) bool

// A filter job works to remove keys from a mapchain with a predicate. It does not
// complete immediately. It removes them in batches, allowing the application to continue
// working while the filter is in progress.
type FilterJob struct {
	source    *Mapchain
	work      [][]string
	predicate FilterPredicate

	// How many keys to process in each call to Run().
	BatchSize int
}

func NewFilterJob(source *Mapchain, predicate FilterPredicate) *FilterJob {
	return &FilterJob{
		source:    source,
		predicate: predicate,
		BatchSize: 100,
	}
}

func (f *FilterJob) Run() (int, int, bool) {
	if len(f.work) == 0 {
		// Initialize the work to do, this is all of the keys in the base node.
		initialWork := []string{}
		for key := range f.source.base {
			initialWork = append(initialWork, key)
		}
		f.work = append(f.work, initialWork)
	}

	processed, removed, done := f.innerWork()
	return processed, removed, done
}

func (f *FilterJob) innerWork() (int, int, bool) {
	depth := 0
	prefix := ""
	processed := 0
	removed := 0
	for processed < f.BatchSize {

		// First we want to traverse to the top of the stack and build the complete prefix.
		if depth < len(f.work)-1 {
			prefix += f.work[depth][0] + "/"
			depth++
			continue
		}

		// Next, we check if the top stack set is empty. If it is, then the first key of
		// the previous set is completed and removed (the current record is the subkeys).
		if len(f.work[depth]) == 0 {
			f.work = f.work[:depth] // pop stack

			if depth == 0 {
				// Nothing left, we are done.
				return processed, removed, true
			}
			depth--
			// Remove last key from prefix.
			// abc/def/
			//     ^^^^  len(parentkey)+1
			prefix = prefix[:len(prefix)-len(f.work[depth][0])-1]
			f.work[depth] = f.work[depth][1:] // Pop first key
			continue
		}

		// Process the next subkey.
		subkey := f.work[depth][0]
		key := prefix + subkey
		value := f.source.getEx(key, true)

		// If it's a node, then we add another set to the stack containing all subkeys.
		if _, ok := value.(Node); ok {
			f.work = append(f.work, []string{})
			prefix += subkey + "/"
			depth++
			for subkey := range value.(Node) {
				f.work[depth] = append(f.work[depth], subkey)
			}
			continue
		}

		// Otherwise, check the key value with the predicate, and remove it if it fails.
		if !f.predicate(f.source.Get(key)) {
			f.source.Set(key, nil)
			removed++
		}
		f.work[depth] = f.work[depth][1:] // Pop the processed key
		processed++
	}

	return processed, removed, false
}
