var Utils = {
	getISODuration: function(text) {
		var iso8601 = /^P(?:(\d+(?:[\.,]\d{0,3})?W)|(\d+(?:[\.,]\d{0,3})?Y)?(\d+(?:[\.,]\d{0,3})?M)?(\d+(?:[\.,]\d{0,3})?D)?(?:T(\d+(?:[\.,]\d{0,3})?H)?(\d+(?:[\.,]\d{0,3})?M)?(\d+(?:[\.,]\d{0,6})?S)?)?)$/;
		var matches = text.match(iso8601);
		if (matches === null) {
			throw '"' + text + '" is an invalid ISO 8601 duration';
		}
		var d = {
			weeks: parseFloat(matches[1], 10),
			years: parseFloat(matches[2], 10),
			months: parseFloat(matches[3], 10),
			days: parseFloat(matches[4], 10),
			hours: parseFloat(matches[5], 10),
			minutes: parseFloat(matches[6], 10),
			seconds: parseFloat(matches[7], 10),
			milliseconds: parseFloat(matches[8], 10)
		};
		$.each(d, function(period, value) {
			d[period] = value ? value : 0;
		});
		return d;
	},

	getSecondsFromDuration: function(d) {
		var seconds = 0;
		seconds += d.years * 365 * 24 * 60 * 60;
		seconds += d.months * 30 * 24 * 60 * 60;
		seconds += d.weeks * 7 * 24 * 60 * 60;
		seconds += d.days * 24 * 60 * 60;
		seconds += d.hours * 60 * 60;
		seconds += d.minutes * 60;
		seconds += d.seconds;
		return seconds;
	},

	formatSeconds: function(seconds) {
		var result = '';
		var hours = parseInt(seconds / 60 / 60);
		if (hours >= 1) {
			result += hours+'h ';
			seconds -= hours * 60 * 60;
		}
		var minutes = parseInt(seconds / 60);
		if (minutes >= 1) {
			result += minutes+'m ';
			seconds -= minutes * 60;
		}
		result += parseInt(seconds)+'s';
		return result;
	},

	formatISODuration: function(d) {
		var hours = 0;
		var minutes = 0;
		var seconds = 0;
		var milliseconds = 0;
		if (d.seconds && d.seconds < 1 && d.seconds > 0) { milliseconds += parseInt(d.seconds * 1000) }
		if (d.seconds) { seconds += parseInt(d.seconds) }
		if (d.minutes) { minutes += parseFloat(d.minutes) }
		if (d.hours) { hours += parseFloat(d.hours) }
		if (d.days) { hours += parseFloat(d.days)     * 24 }
		if (d.weeks) { hours += parseFloat(d.weeks)   * 24 * 7 }
		if (d.months) { hours += parseFloat(d.months) * 24 * 7 * 4 }
		if (d.years) { hours += parseFloat(d.years)   * 24 * 7 * 4 * 12 }
		seconds = seconds.toString();
		minutes = minutes.toString();
		hours = hours.toString();

		if (hours == '0' && minutes == '0' && seconds == '0') {
			return milliseconds + 'ms';
		}
		if (hours == '0' && minutes == '0') {
			return seconds + 's';
		}
		if (hours == '0') {
			return minutes + 'm ' + seconds + 's';
		}
		return hours + 'h ' + minutes + 'm ' + seconds + 's';
	}
};

module.exports = Utils;