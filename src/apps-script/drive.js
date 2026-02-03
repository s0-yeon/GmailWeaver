function onDriveItemsSelected(e) {
  var driveSection = CardService.newCardSection()
      .setHeader('📁 드라이브 파일 2개');
  
  var files = DriveApp.getFiles();
  var fileCount = 0;
  while (files.hasNext() && fileCount < 2) {
    var file = files.next();
    driveSection.addWidget(
      CardService.newTextParagraph()
        .setText((fileCount + 1) + '. ' + file.getName())
    );
    fileCount++;
  }
  
  return CardService.newCardBuilder()
      .addSection(driveSection)
      .build();
}
