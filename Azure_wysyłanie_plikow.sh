#!/usr/bin/env bash
set -euo pipefail

##############################################
## KONFIGURACJA
##############################################

BASE_DIR="/mnt/data"
TESTFILES_DIR="${BASE_DIR}/testfiles"
RESULTS_CSV="${BASE_DIR}/upload_results_azure.csv"

AZ_ACCOUNT_NAME="starchfrclab01"                  # Storage Account
AZ_CONTAINERS=("hot" "cool" "archive")            # tylko te dwa
AZ_TIERS=("Hot" "Cool" "Archive")                 # odpowiadające tier-y

FILES=(
  "test_20GB.bin:20G"
  "test_50GB.bin:50G"
  "test_100GB.bin:100G"
)

RUNS_PER_FILE=3

##############################################
## FUNKCJE
##############################################
check_deps() {
  local deps=("az" "stat" "bc" "truncate")
  for d in "${deps[@]}"; do
    command -v "$d" >/dev/null 2>&1 || { echo "ERROR: Brak narzędzia: $d"; exit 1; }
  done
  command -v azcopy >/dev/null 2>&1 || {
    echo "ERROR: Brak 'azcopy'. Zainstaluj azcopy (wtedy masz progress + MB/s)."
    exit 1
  }
}

now_ns(){ date +%s%N; }
elapsed_seconds(){ echo "scale=6; ($2 - $1) / 1000000000" | bc; }
calc_throughput_mb_s(){
  local size_bytes="$1"; local elapsed_s="$2"
  [[ "$elapsed_s" == "0" ]] && echo "0" || echo "scale=3; ($size_bytes / 1024 / 1024) / $elapsed_s" | bc
}

write_csv_header() {
  if [[ ! -f "$RESULTS_CSV" ]]; then
    echo "provider,storage_tier_or_class,operation,object_path,run_index,bytes,elapsed_s,throughput_MBps" > "$RESULTS_CSV"
  fi
}

append_csv_row() {
  # provider=AZURE
  echo "AZURE,$1,$2,$3,$4,$5,$6,$7" >> "$RESULTS_CSV"
}

create_test_files() {
  mkdir -p "$TESTFILES_DIR"
  for entry in "${FILES[@]}"; do
    local name="${entry%%:*}"
    local size="${entry##*:}"
    local path="${TESTFILES_DIR}/${name}"
    if [[ ! -f "$path" ]]; then
      echo "Tworzę plik: $path ($size) [sparse]"
      truncate -s "$size" "$path"
    fi
  done
}

delete_blob_if_exists() {
  local container="$1"
  local blob="$2"
  az storage blob delete \
    --account-name "$AZ_ACCOUNT_NAME" \
    --container-name "$container" \
    --name "$blob" \
    --auth-mode login \
    >/dev/null 2>&1 || true
}

upload_one() {
  local local_file="$1"
  local container="$2"
  local tier="$3"
  local run="$4"

  local blob_name
  blob_name="$(basename "$local_file")"

  # Kasujemy poprzednią wersję obiektu, żeby ten sam plik mógł być wrzucony 3 razy
  delete_blob_if_exists "$container" "$blob_name"

  local dst_url="https://${AZ_ACCOUNT_NAME}.blob.core.windows.net/${container}/${blob_name}"
  local logical_path="${AZ_ACCOUNT_NAME}/${container}/${blob_name}"

  local size_bytes start_ns end_ns elapsed_s throughput
  size_bytes=$(stat -c%s "$local_file")
  start_ns=$(now_ns)

  echo "=== [AZURE] Upload run ${run}: ${local_file} -> ${logical_path} (tier: ${tier}) ==="
  azcopy copy "$local_file" "$dst_url" --overwrite=true --block-blob-tier="$tier"

  end_ns=$(now_ns)
  elapsed_s=$(elapsed_seconds "$start_ns" "$end_ns")
  throughput=$(calc_throughput_mb_s "$size_bytes" "$elapsed_s")

  echo "Czas: ${elapsed_s}s | Rozmiar: ${size_bytes}B | Śr. pasmo: ${throughput} MB/s"
  append_csv_row "$tier" "UPLOAD" "$logical_path" "$run" "$size_bytes" "$elapsed_s" "$throughput"
}

main() {
  check_deps
  write_csv_header
  create_test_files

  echo "INFO: Upewnij się, że jesteś zalogowany:"
  echo "  az login"
  echo "  azcopy login"
  echo

  for ((run=1; run<=RUNS_PER_FILE; run++)); do
    for entry in "${FILES[@]}"; do
      local name="${entry%%:*}"
      local path="${TESTFILES_DIR}/${name}"

      for i in "${!AZ_CONTAINERS[@]}"; do
        upload_one "$path" "${AZ_CONTAINERS[$i]}" "${AZ_TIERS[$i]}" "$run"
      done
    done
  done

  echo "Zakończono Azure. Wyniki: $RESULTS_CSV"
}

main "$@"
