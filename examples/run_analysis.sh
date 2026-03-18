#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e 
shopt -s extglob

# ---------------------------------------------------------
# Parse arguments to choose the scenario and controls
# ---------------------------------------------------------
SCENARIO="filesets" # Default
PAUSE=true          # Default to pausing between steps

for arg in "$@"; do
    case $arg in
        --inputs)
            SCENARIO="inputs"
            ;;
        --filesets)
            SCENARIO="filesets"
            ;;
        --no-pause)
            PAUSE=false
            ;;
        *)
            echo "Usage: ./run_analysis.sh [--filesets | --inputs] [--no-pause]"
            exit 1
            ;;
    esac
done

# ---------------------------------------------------------
# Helper function to pause the script
# ---------------------------------------------------------
wait_for_user() {
    local next_step="$1"
    
    if [ "$PAUSE" = true ]; then
        echo ""
        # Read user input into the 'choice' variable
        read -p "Press [Enter] to start: $next_step (or type 'q' to quit)... " choice
        
        # If the user typed q or Q, exit the script cleanly
        if [[ "$choice" == "q" || "$choice" == "Q" ]]; then
            echo ""
            echo "Stoped!"
            exit 0
        fi
        echo "------------------------------------------------------"
    fi
}

echo "======================================================"
echo "          YDANA-HEP CLI Workflow Example                  "
echo "======================================================"
echo "Note: This script should be run from the 'examples/' directory."
if [ "$PAUSE" = false ]; then
    echo "Running in non-stop (--no-pause) mode."
fi
echo ""

if [ "$SCENARIO" == "filesets" ]; then
    # =========================================================
    # SCENARIO A: Driven entirely by YAML filesets (Default)
    # =========================================================
    echo "[1/3] Dumping events (Reading datasets from config)..."
    ydana dump-events -cf yamls/run_config.yaml -o results_filesets
    
    # Tell the function what the next step is!
    wait_for_user "Step 2/3 - Filling Histograms"

    echo "[2/3] Filling histograms (Reading datasets from config)..."
    ydana fill-histos -cf yamls/run_config.yaml -o results_filesets --per-partition
    
    # Tell the function what the next step is!
    wait_for_user "Step 3/3 - Merging Root Files"
    
    echo "[3/3] Merging events and histogram files..."
    ydana merge-roots -i results_filesets/roots/!(*DY*).root -o results_filesets/ --tag _Zprime
    ydana merge-histos -i results_filesets/histograms/!(*DY*).root -o results_filesets/ --tag _Zprime
    ydana merge-roots -i results_filesets/roots/!(*Zprime*).root -o results_filesets/ --tag _DY
    ydana merge-histos -i results_filesets/histograms/!(*Zprime*).root -o results_filesets/ --tag _DY

else
    # =========================================================
    # SCENARIO B: Driven by explicit CLI inputs
    # =========================================================
    
    echo "[1/3] Dumping events (Using explicit --inputs)..."
    ydana dump-events -cf yamls/run_config.yaml --inputs "ntuples/DY/*.root" --tree "dtNtupleProducer/DTTREE" -o results_DY --tag _DY

    wait_for_user "Step 2/3 - Filling Histograms"

    echo "[2/3] Filling histograms (Using explicit --inputs)..."
    ydana fill-histos -cf yamls/run_config.yaml --inputs "ntuples/DY/*.root" --tree "dtNtupleProducer/DTTREE" -o results_DY --tag _DY --per-partition
    
    wait_for_user "Step 3/3 - Merging Root Files"
    
    echo "[3/3] Merging events and histogram files..."
    ydana merge-roots -i "results_DY/roots/*.root" -o results_DY/ --tag _DY
    ydana merge-histos -i "results_DY/histograms/*.root" -o results_DY/ --tag _DY
fi

echo ""
echo "======================================================"
echo " Analysis complete!"
echo "======================================================"