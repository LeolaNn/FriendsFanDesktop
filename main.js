const { app, BrowserWindow, ipcMain, dialog, Menu } = require('electron');
app.commandLine.appendSwitch('disable-features', 'WebRtcHideLocalIpsWithMdns');
function create() {
  const w = new BrowserWindow({ width: 760, height: 660, backgroundColor: '#121216',
    webPreferences: { nodeIntegration: true, contextIsolation: false, webSecurity: false } });
  Menu.setApplicationMenu(null);
  w.loadFile('index.html');
}
app.whenReady().then(create);
app.on('window-all-closed', () => app.quit());
ipcMain.handle('pick', async (e, filters) => {
  const r = await dialog.showOpenDialog({ properties: ['openFile'],
    filters: filters || [{ name: 'Відео', extensions: ['mkv', 'mp4', 'avi', 'webm', 'mov', 'm4v', 'ts', 'm2ts', 'flv', 'wmv', 'ogm'] }, { name: 'Усі файли', extensions: ['*'] }] });
  return r.canceled ? null : r.filePaths[0];
});
