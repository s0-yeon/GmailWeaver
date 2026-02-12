function doGet(e) {
  return HtmlService.createHtmlOutputFromFile("index")
    .setTitle("Gmail Network Graph");
}