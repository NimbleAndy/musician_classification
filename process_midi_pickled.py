import os
import pickle
import pretty_midi
from ipyparallel import Client
import os

def gather_midi_files(base_directory, composers):
    midi_files = []
    composer_counts = {}
    
    # Sort composers alphabetically to match the sorted list, this was a big issue when reading in the directory.
    # Because the function y = label_encoder(set(data_dict[composer].values)) must be consistent with the order of how the folders are presented.

    composers = sorted(composers)
    print("\nprocessing composers in order:", composers)
    
    for composer in composers:
        composer_dir = os.path.join(base_directory, composer)
        if not os.path.isdir(composer_dir):
            print(f"cposer folder not found: {composer_dir}") #Adding debug statements because the script would crash without any feedback
            continue
        
        composer_files = []
        for f in os.listdir(composer_dir):
            full_path = os.path.join(composer_dir, f)
            if os.path.isfile(full_path) and f.lower().endswith(".mid"):
                midi_files.append(full_path)
                composer_files.append(full_path)
            else:
                print(f"skipping invalid file or flder: {full_path}")
        
        composer_counts[composer] = len(composer_files)
    
    print("\ninitial files per composer:")
    for composer in sorted(composer_counts.keys()):
        print(f"{composer}: {composer_counts[composer]} MIDI files")
    
    print(f"\ntotal MIDI files found: {len(midi_files)}")
    return midi_files
#This is 

def extract_midi_details(midi_file):
    try:
        midi_data = pretty_midi.PrettyMIDI(midi_file)

        
        notes_list = []
        note_lengths = []
        octaves = []

        for instrument in midi_data.instruments:
            for note in instrument.notes:
                notes_list.append(pretty_midi.note_number_to_name(note.pitch))
                note_lengths.append(note.end - note.start)
                octaves.append(note.pitch // 12 - 1)

        
        notes_with_details = sorted(
            zip(notes_list, note_lengths, octaves, [note.start for instrument in midi_data.instruments for note in instrument.notes]),
            key=lambda x: x[3]
        )

        notes_list = [n[0] for n in notes_with_details]
        note_lengths = [n[1] for n in notes_with_details]
        octaves = [n[2] for n in notes_with_details]

        
        offsets, bpms = midi_data.get_tempo_changes()
        tempos = [{"offset": offset, "bpm": bpm} for offset, bpm in zip(offsets, bpms)]

        
        time_signatures = [
            {"offset": ts.time, "numerator": ts.numerator, "denominator": ts.denominator}
            for ts in midi_data.time_signature_changes
        ]

        
        num_notes = len(notes_list)

        
        key_signature = (
            midi_data.key_signature_changes[0].key_number
            if midi_data.key_signature_changes else None
        )

        return {
            "notes": notes_list,
            "note_lengths": note_lengths,
            "octaves": octaves,
            "overall_key": key_signature,
            "num_notes": num_notes,
            "tempos": tempos,
            "musical_meters": time_signatures,
        }

    except Exception as e:
        return {"error": str(e), "file": midi_file}


def process_midi_files_parallel(midi_files):
    results = {}
    processed_counts = {}
    
    for midi_file in midi_files:
        composer = os.path.basename(os.path.dirname(midi_file))
        midi_name = os.path.splitext(os.path.basename(midi_file))[0]

        if composer not in results:
            results[composer] = {}
            processed_counts[composer] = 0
            
        results[composer][midi_name] = extract_midi_details(midi_file)
        processed_counts[composer] += 1
    
    print(f"\nProcessed files in this chunk:")
    for composer, count in processed_counts.items():
        print(f"{composer}: {count} files")
        
    return results

if __name__ == "__main__":
    
    COMPOSERS = sorted(["handel", "alkan", "schubert", "mozart", "scarlatti", "victoria"])

    
    base_directory = "C:/Users/Sileo/COLLEGE/FALL24/CST 4702/FinalProjectML/project/dataset"

    
    midi_files = gather_midi_files(base_directory, COMPOSERS)

    
    rc = Client()
    dview = rc[:]

    with dview.sync_imports():
        import pretty_midi
        import os
        import pickle

    
    dview.push({
        "extract_midi_details": extract_midi_details,
        "process_midi_files_parallel": process_midi_files_parallel
    })

    
    num_workers = len(rc)
    print(f"Number of workers (engines): {num_workers}")

    if num_workers == 0:
        raise RuntimeError("No ipyparallel engines found. Please start your cluster with 'ipcluster start -n X'.")

    chunks = [midi_files[i::num_workers] for i in range(num_workers)]
    print("Processing in parallel...")

    
    results_list = dview.map_sync(process_midi_files_parallel, chunks)

    
    final_data = {}
    for partial_dict in results_list:
        for composer, files in partial_dict.items():
            if composer not in final_data:
                final_data[composer] = {}
            final_data[composer].update(files)

    
    output_path = "midi_data_all.pkl"
    with open(output_path, "wb") as f:
        pickle.dump(final_data, f)

    print(f"Processing complete. Results saved to {output_path}")
    
    # After merging results
    print("\nFinal processed files per composer:")
    for composer in sorted(final_data.keys()):
        valid_files = sum(1 for f in final_data[composer].values() if "error" not in f)
        error_files = sum(1 for f in final_data[composer].values() if "error" in f)
        print(f"{composer}: {len(final_data[composer])} total files ({valid_files} valid, {error_files} errors)")