var NodeGraph = require('./NodeGraph.jsx');
var Utils = require('../utils.js');

var GroupContainer = React.createClass({
	getGroupSize: function(count) {
		console.log(count);
		var ratio = 0.8;
		var numHoriz = parseInt(Math.sqrt(count/ratio));
		var width = 100/numHoriz;
		var numVert = Math.ceil(count / numHoriz);
		return [width, 100 / numVert];
	},
	render: function() {
		var that = this;
		var groupSize = that.getGroupSize(Object.keys(that.props.groups).length);
		var groupStyle = {
			'width': groupSize[0]+'%',
			'height': groupSize[1]+'%'
		};
		return (
			<div className="groups-container">
				{$.map(Object.keys(that.props.groups).sort(), function (groupName) {
					var group = that.props.groups[groupName];
					var groupTitle = groupName.replace(/([A-Z])/g, ' $1');
					groupTitle = groupTitle.split('-')[0].split(' ');
					var groupType = groupTitle.splice(groupTitle.length-1, 1).join(' ').toLowerCase();
					groupTitle = groupTitle.join(' ');
					var groupIcon = {
						'ramp': 'road',
						'intersection': 'exchange',
						'tap': 'road',
						'transformer': 'exchange'
					}[groupType];

					var groupColor = {
						'ramp': 'blue',
						'intersection': 'green',
						'tap': 'blue',
						'transformer': 'green'
					}[groupType];

					var groupClass = "fa fa-" + groupIcon;
					var circleClass = "circle-icon circle-icon-" + groupColor;

					var avg_time = Utils.formatISODuration(Utils.getISODuration(group['avg_time_taken']));
					return (
						<div className="group" key={groupName} style={groupStyle}>
							<div className="group-content">
								<div className="group-content-inner">
									<h1><span className={circleClass}><i className={groupClass}></i></span>{groupTitle}</h1>
									<ul className="status-icons">
										{$.map(Object.keys(group['processes']), function (processName) {
											var process_dict =  group['processes'][processName];
											var icon = '';
											switch(process_dict['state']) {
												case "available":
													icon = 'fa-check-circle';
													break;
												case "busy":
													icon = 'fa-cog fa-spin';
													break;
												case "overloaded":
													icon = 'fa-exclamation-triangle flash';
													break;
											}
											var spanClassName = 'fa ' + icon;
											var url = '/detail/' + processName + '/';
											return (
												<li key={processName}>
													<a href={url}>
														<span className={spanClassName} title={processName}></span>
													</a>
												</li>
											)
										})}
									</ul>
									<div className="stats">
										<div className="col">
											<span className="label">Items in queue</span>
											{group['waiting']}
										</div>
										<div className="col">
											<span className="label">Avg/item</span>
											<span className="small">x&#772;:</span> {avg_time}
										</div>
									</div>
								</div>
							<NodeGraph histogram={group['histogram']} lastMinutes={that.props.lastMinutes} />
							</div>
						</div>
					)
				})}
			</div>
		)
	}
});

module.exports = GroupContainer;