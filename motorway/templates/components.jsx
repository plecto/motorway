var NodesContainer = React.createClass({

	getNodeSize: function(count) {
		var ratio = 0.8;
		var numHoriz = parseInt(Math.sqrt(count/ratio));
		var width = 100/numHoriz;
		var numVert = Math.ceil(count / numHoriz);
		return [width, 100 / numVert];
	},

	render: function() {
		// node size
		var nodeSize = this.getNodeSize(this.props.nodes.length);
		var nodeStyle = {
			'width': nodeSize[0]+'%',
			'height': nodeSize[1]+'%'
		};

		return (
			<div className="nodes-container">
			{$.map(this.props.nodes, function (node) {
				// node status class
				var nodeClass = 'node';
				if (node.waiting < 1) {
					nodeClass += ' node-status-zero';
				} else if (node.waiting < 10) {
					nodeClass += ' node-status-low';
				}
				if (node.secondsRemaining > PipelineSettings.remainingSecondsDanger) {
					nodeClass += ' node-status-danger';
				} else if (node.secondsRemaining > PipelineSettings.remainingSecondsWarning) {
					nodeClass += ' node-status-warning';
				}

				return (
					<div key={node.name} className={nodeClass} style={nodeStyle}>
						<div className="node-inner">
							<NodeCircle size={node.secondsRemaining} />
							<div className="node-content">
								<span className="node-icon"><span className={node.iconClass}></span></span>
								<a href={node.url} target="_blank" className="node-name">{node.title} {node.status}2</a>
								<p className="node-type">{node.nodeType}</p>
								<div className="node-time">
									<p className="node-time-average">x&#772;: {Utils.formatISODuration(node.avgTime)}</p>
                                    <p className="node-time-percentile">95%: {Utils.formatISODuration(node.percentile)}</p>
								</div>
								<div className="node-waiting">
									<p className="node-waiting-main">{node.waiting} / {Utils.formatSeconds(node.secondsRemaining)}</p>
									<h2 className="node-waiting-alt">&#35; Waiting / Est. time to process</h2>
								</div>
							</div>
							<NodeGraph items={node.latestHistogram} />
						</div>
					</div>
				)
			})}
			</div>
		)
	}
});

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
											return (
												<li key={processName}>
													<span className={spanClassName} title={processName}></span>
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

var NodeCircle = React.createClass({
	render: function() {
		var size = parseInt(this.props.size);
		var style = {
			'width': size,
			'height': size,
			'marginTop': -parseInt(size/2) - 20,
			'marginLeft': -parseInt(size/2) + 20
		};
		return (
			<div className="node-circle" style={style}></div>
		)
	}
});

var NodeGraph = React.createClass({
	render: function() {
		var histogram = this.props.histogram;

		var maxHistogramValue = 0;
		$.each(histogram, function(minute, item) {
			maxHistogramValue = Math.max(maxHistogramValue, (item['success_count'] + item['error_count'] + item['timeout_count']));
		});

		$.each(histogram, function(minute, item) {
			if (maxHistogramValue > 0) {
				histogram[minute]['success_percentage'] = (item['success_count'] / maxHistogramValue) * 100;
				histogram[minute]['error_percentage'] = (item['error_count'] / maxHistogramValue) * 100;
				histogram[minute]['timeout_percentage'] = (item['timeout_count'] / maxHistogramValue) * 100;
			} else {
				histogram[minute]['success_percentage'] = 0;
				histogram[minute]['error_percentage'] = 0;
				histogram[minute]['timeout_percentage'] = 0;
			}
		});
		var latestHistogram = this.props.lastMinutes.map(function(minute) {
			return {
				'minute': minute,
				'value': histogram[minute]
			};
		});
		return (
			<div className="node-graph">
				{latestHistogram.map(function (item) {
					var blank = {
						'height': parseInt(100-parseInt(item.value.success_percentage)-parseInt(item.value.error_percentage)-parseInt(item.value.timeout_percentage))+'%'
					};
					var success = {
						'height': parseInt(item.value.success_percentage)+'%'
					};
					var timeout = {
						'height': parseInt(item.value.timeout_percentage)+'%'
					};
					var error = {
						'height': parseInt(item.value.error_percentage)+'%'
					};
					return <span className="node-graph-bar" key={item.minute}>
						<span className="node-graph-bar-part node-graph-blank" style={blank}></span>
						<span className="node-graph-bar-part node-graph-success" style={success}>{item.value.success_count}</span>
						<span className="node-graph-bar-part node-graph-timeout" style={timeout}>{item.value.timeout_count}</span>
						<span className="node-graph-bar-part node-graph-error" style={error}>{item.value.error_count}</span>
					</span>
				})}
			</div>
		)
	}
});
