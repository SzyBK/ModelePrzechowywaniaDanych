#!/usr/bin/env bash
set -euo pipefail

##############################################
## KONFIGURACJA – PODMIEŃ NA SWOJE DANE
##############################################

# Azure
AZ_ACCOUNT_NAME="starchfrclab01"          # <-- Twoje konto storage
AZ_CONTAINER_HOT="hot"                    # <-- kontener z warstwą Hot
AZ_CONTAINER_HOT="cool"                   # <-- kontener z warstwą Cool
AZ_CONTAINER_HOT="archive"                # <-- kontener z warstwą Archive

# Opis testowanej warstwy (można później zmienić na "Cool", "Archive" itd.)
AZ_STORAGE_TIER="Hot"

# Lista blobów do testu w Azure (pełne nazwy blobów w kontenerze)
AZ_BLOBS=(
  "file-20GB.bin"
  "file-50GB.bin"
  "file-100GB.bin"
)

# Ilu powtórzeń dla każdego pliku (dla uśrednienia wyników)
RUNS_PER_FILE=3

# Katalog na pobrane pliki (lokalny na runnerze)
DOWNLOAD_DIR="./downloads_azure"

# Plik wynikowy (CSV) – kolejne uruchomienia będą DOPISYWAĆ wiersze
RESULTS_CSV="./download_results.csv"

##############################################
## FUNKCJE POMOCNICZE
##############################################

check_deps() {
  local deps=("az" "stat" "sha256sum" "bc")
  for d in "${deps[@]}"; do
    if ! command -v "$d" >/dev/null 2>&1; then
      echo "ERROR: Brak narzędzia: $d. Zainstaluj je przed uruchomieniem."
      exit 1
    fi
  done
}

now_ns() { date +%s%N; }

elapsed_seconds() {
  local start_ns="$1"
  local end_ns="$2"
  echo "scale=6; ($end_ns - $start_ns) / 1000000000" | bc
}

calc_throughput_mb_s() {
  local size_bytes="$1"
  local elapsed_s="$2"
  if [[ "$elapsed_s" == "0" ]]; then
    echo "0"
  else
    echo "scale=3; ($size_bytes / 1024 / 1024) / $elapsed_s" | bc
  fi
}

write_csv_header() {
  # Tworzymy nagłówek TYLKO jeśli plik jeszcze nie istnieje – nie nadpisujemy wyników
  if [[ ! -f "$RESULTS_CSV" ]]; then
    echo "provider,storage_tier_or_class,file_name,file_logical_path,run_index,bytes,elapsed_s,throughput_MBps,sha256" > "$RESULTS_CSV"
  fi
}

append_csv_row() {
  local provider="$1"
  local storage_tier="$2"
  local file_name="$3"
  local logical_path="$4"
  local run_index="$5"
  local bytes="$6"
  local elapsed_s="$7"
  local throughput="$8"
  local sha="$9"

  echo "${provider},${storage_tier},${file_name},${logical_path},${run_index},${bytes},${elapsed_s},${throughput},${sha}" >> "$RESULTS_CSV"
}

##############################################
## POBIERANIE Z AZURE BLOB (Hot)
##############################################

download_from_azure() {
  local blob_name="$1"
  local run_index="$2"

  local local_path="${DOWNLOAD_DIR}/azure-${blob_name//\//_}"

  echo
  echo "=== [Azure] Pobieranie bloba: ${blob_name} (tier: ${AZ_STORAGE_TIER}, run ${run_index}) ==="

  local start_ns end_ns elapsed_s size_bytes throughput sha

  start_ns=$(now_ns)

  az storage blob download \
    --auth-mode login \
    --account-name "$AZ_ACCOUNT_NAME" \
    --container-name "$AZ_CONTAINER_HOT" \
    --name "$blob_name" \
    --file "$local_path" \
    --no-progress >/dev/null

  end_ns=$(now_ns)

  elapsed_s=$(elapsed_seconds "$start_ns" "$end_ns")
  size_bytes=$(stat -c%s "$local_path")
  throughput=$(calc_throughput_mb_s "$size_bytes" "$elapsed_s")
  sha=$(sha256sum "$local_path" | awk '{print $1}')

  echo "Czas pobrania  : ${elapsed_s} s"
  echo "Rozmiar        : ${size_bytes} B"
  echo "Średnie pasmo  : ${throughput} MB/s"
  echo "SHA256         : ${sha}"

  append_csv_row "Azure" "$AZ_STORAGE_TIER" "$(basename "$blob_name")" "$blob_name" "$run_index" "$size_bytes" "$elapsed_s" "$throughput" "$sha"
}

##############################################
## GŁÓWNY PRZEPŁYW
##############################################

main() {
  check_deps
  mkdir -p "$DOWNLOAD_DIR"
  write_csv_header()

  echo "========================================="
  echo "Test pobierania plików z Azure Blob (tier: $AZ_STORAGE_TIER)"
  echo "Wyniki w CSV (dopisywane): $RESULTS_CSV"
  echo "========================================="

  for blob in "${AZ_BLOBS[@]}"; do
    for ((i=1; i<=RUNS_PER_FILE; i++)); do
      download_from_azure "$blob" "$i"
    done
  done

  echo
  echo "Zakończono test Azure."
}

main "$@"
