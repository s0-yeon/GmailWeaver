function resetLastSync() {
  PropertiesService.getUserProperties().deleteProperty("GW_LAST_SYNC_MS");
  Logger.log("GW_LAST_SYNC_MS 초기화 완료");
}