// this module is just for configuring target information, where the radar data can be found. This specific viewer is built for 3D radar data of the German weather service DWD which is stored in folder for each site

module.exports = {
	targetUrl: "https://opendata.dwd.de/weather/radar/sites/pz/", // location to get data from
	listFileName: "dir.list", // name of file to poll
	dataDir: "data", // directory to write data to
	pollInterval: 120, // seconds between polls
	doublePollFileSize: false,
}

try{
	var configLocal = require("./config-local.js")
	for(var x in configLocal){
		module.exports[x] = configLocal[x]
	}
}catch(a){}

//console.log(module.exports)