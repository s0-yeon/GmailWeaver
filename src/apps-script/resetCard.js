function resetAndGoHome() {
  const props = PropertiesService.getScriptProperties();
  props.setProperty("SYNC_STATUS", "stopped"); // 상태 초기화
  console.log("초기화 완료! 이제 Gmail을 새로고침하세요.");
}