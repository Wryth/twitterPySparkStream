var map = {};
var markers =[];
var myList = [];
var colorSchema = ["Yellow Dot","Blue Dot","Green Dot","Light Blue Dot", "Orange Dot", "Pink Dot", "Purple Dot", "Red Dot","Plain Yellow","Plain Blue","Plain Green","Plain Light Blue","Plain Orange","Plain Pink", "Plain Purple","Plain Red"];

function initMap() {
		map = new google.maps.Map(document.getElementById("map"), {
	});
}

function initE_Raw(){
	initMarkerMap(exampleRaw);
}

function initE_Cluster(){
	initMarkerCluster(exampleCluster);
}


function initL_Cluster(){
	initMarkerCluster(tweets);
}

 
function initMarkerMap(array){
	deleteMarkers();
	bounds = new google.maps.LatLngBounds();
	for(var i = 0; i < array.length; i++) {
		loc = new google.maps.LatLng(array[i].tweet[0].tweet_location[1],array[i].tweet[0].tweet_location[0]);
		bounds.extend(loc);
		var marker = new google.maps.Marker({
			position: loc,
			map:map,
			title: array[i].tweet[0].tweet_text
		});
		markers.push(marker);
	}
	map.fitBounds(bounds);
	map.panToBounds(bounds);
}

function setColor(id) {
	var icon = "http://maps.google.com/mapfiles/ms/icons/";
	switch(id){
		case 0:
			icon = icon + "yellow-dot.png";
			break;
		case 1:
			icon = icon + "blue-dot.png";
			break;
		case 2:
			icon = icon + "green-dot.png";
			break;
		case 3:
			icon = icon + "ltblue-dot.png";
			break;
		case 4:
			icon = icon + "orange-dot.png";
			break;
		case 5:
			icon = icon + "pink-dot.png";
			break;
		case 6:
			icon = icon + "purple-dot.png";
			break;
		case 7:
			icon = icon + "red-dot.png";
			break;
		case 8:
			icon = icon + "yellow.png";
			break;
		case 9: 
			icon = icon + "blue.png";
			break;
		case 10: 
			icon = icon + "green.png";
			break;
		case 11:
			icon = icon + "lightblue.png";
			break;
		case 12:
			icon = icon + "orange.png";
			break;
		case 13:
			icon = icon + "pink.png";
			break;
		case 14: 
			icon = icon +"purple.png";
			break;
		case 15:
			icon = icon + "red.png";
			break;
		}
		return icon;
	}
function initMarkerCluster(array){
	deleteMarkers();
	bounds = new google.maps.LatLngBounds();
	for(var j = 0; j< array.length; j++) {
		loc = new google.maps.LatLng(array[j].tweet_location[1],array[j].tweet_location[0]);
		bounds.extend(loc);
		var marker = new google.maps.Marker({
			position: loc,
			map:map,
			title: array[j].tweet_text,
			icon: setColor(array[j].prediction)
		});
		markers.push(marker);
	}
	map.fitBounds(bounds);
	map.panToBounds(bounds);
}

function deleteMarkers() {
	for(var i = 0; i<markers.length;i++) {
		markers[i].setMap(null);
	}
	markers = [];
}
