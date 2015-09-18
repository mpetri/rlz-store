var classesNumber = 9,
    cellSize = 16;
var colors;
var colorScale;
var legendElementWidth = cellSize * 5;
var w = window,
    d = document,
    e = d.documentElement,
    g = d.getElementsByTagName('body')[0],
    x = w.innerWidth || e.clientWidth || g.clientWidth,
    y = w.innerHeight|| e.clientHeight|| g.clientHeight;

var cellsPerRow = Math.floor(x / cellSize)
var numRows = Math.floor((y-200) / cellSize)
var numCells = numRows * cellsPerRow
var dataUrlPrefix

var b = {
  w: 75, h: 30, s: 3, t: 10
};

var entityMap = {
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': '&quot;',
    "'": '&#39;',
    "/": '&#x2F;'
  };

  function escapeHtml(string) {
    return String(string).replace(/[&<>"'\/]/g, function (s) {
      return entityMap[s];
    });
  }

var largeScale = ['#fff','#fef3f3','#fde3e3','#fdd3d3','#fcc3c3','#fbb3b3','#faa3a3','#f99393','#f88383','#f77373','#f66363','#f55353','#f44342','#f43332','#f32322','#f21312','#e60e0d','#d60d0c','#c60c0b','#b60b0a','#a60a09','#960908','#860807','#760707','#650606','#550505','#450404','#350303','#250202','#150101','#00000'];

//#########################################################
function heatmap_display(url, heatmapId, paletteName) {
    //==================================================
    
    colors = colorbrewer[paletteName][classesNumber];
    colorScale = d3.scale.linear()
            .domain([0,100*1000*1000])
            .range(colors);
    var svg = d3.select(heatmapId).append("svg")
            .attr("width", x)
            .attr("height", y)
            .append("g");

    dataUrlPrefix = url
    fillData(url);
};

//==================================================
function fillData(url) {
    d3.json(url+"?start=0&end=0&numcells="+numCells, function(error,json) {
        if (error) return console.warn(error);
        data = json;
        var arr = data.data;
        var row_number = arr.length;
        var col_number = arr[0].length;
        var svg = d3.select("svg");

        svg.append('defs')
            .append('pattern')
            .attr('id', 'diagonalHatch')
            .attr('patternUnits', 'userSpaceOnUse')
            .attr('width', 4)
            .attr('height', 4)
            .append('path')
            .attr('d', 'M-1,1 l2,-2 M0,4 l4,-4 M3,5 l2,-2')
            .attr('stroke', '#000000')
            .attr('stroke-width', 1);

        var cells = svg.selectAll(".cell")
            .data(data.data)
            .enter()
            .append("svg:rect")
            .attr("x", function(d) {
                return d.id%cellsPerRow * cellSize;
            })
            .attr("y", function(d) {
                numRows = Math.floor(d.id/cellsPerRow)
                return numRows * cellSize;
            })
            .attr("rx", 4)
            .attr("ry", 4)
            .attr("class", function(d, i, j) {
                return "cell bordered cr" + j + " cc" + i;
            })
            .attr("width", cellSize)
            .attr("height", cellSize)
            .style("fill", function(d) {
                if (d != null) return colors[d.quantile];
                //if (d != null) return colorScale(d.freq);
                else return "url(#diagonalHatch)";
            })
            .on("mouseover", function(d){
               //highlight text
               d3.select(this).classed("cell-selected",true);
               if (d.bytes_per_cell <= 500 ) {
                    d3.select("#tooltip")
                    .style("left", (d3.event.pageX+20) + "px")
                    .style("top", (d3.event.pageY-20) + "px")
                    .select("#value")
                    .html("<strong>bytes per cell:</strong> "+d.bytes_per_cell+
                        "<br/><strong>start:</strong> "+d.start+"<br/><strong>stop:</strong> "+
                        d.stop+"<br/><strong>freq:</strong> "+d.freq+"<br/>"+
                        "<strong>content:</strong> '"+escapeHtml(d.content)+"'<br/>"
                        );
               } else {
                    
                    d3.select("#tooltip")
                    .style("left", (d3.event.pageX+20) + "px")
                    .style("top", (d3.event.pageY-20) + "px")
                    .select("#value")
                    .html("<strong>bytes per cell:</strong> "+d.bytes_per_cell+
                        "<br/><strong>start:</strong> "+d.start+"<br/><strong>stop:</strong> "+
                        d.stop+"<br/><strong>freq:</strong> "+d.freq+"<br/>"
                        );
               }
               //Show the tooltip
               d3.select("#tooltip").classed("hidden", false);
            })
            .on("mouseout", function(){
                   d3.select(this).classed("cell-selected",false);
                   d3.select("#tooltip").classed("hidden", true);
            })
            .on("click", function(d) {
                addBreadCrumb("["+d.start+"-"+d.stop+"]",d.start,d.stop,numCells)
                var url = dataUrlPrefix
                updateData(url+"?start="+d.start+"&end="+d.stop+"&numcells="+numCells);
                d3.event.stopPropagation();
            });

        var legend = svg.append("g")
            .attr("class", "legend")
            .attr("transform", "translate(0,-300)")
            .selectAll(".legendElement")
            .data([0,"<=10","<=100","<=1000","<=10k","<=100k","<=1M","<=10M","<=100M",">100M"])
            .enter().append("g")
            .attr("class", "legendElement");

        legend.append("svg:rect")
            .attr("x", function(d, i) {
                return 20 + legendElementWidth * i;
            })
            .attr("y", y + 150)
            .attr("class", "cellLegend bordered")
            .attr("width", legendElementWidth)
            .attr("height", cellSize *2 )
            .style("fill", function(d, i) {
                //return colorScale(i);
                return colors[i];
            });

        legend.append("text")
            .attr("class", "mono legendElement")
            .text(function(d) {
                return d;
            })
            .attr("x", function(d, i) {
                return 20 + legendElementWidth * i;
            })
            .attr("y", y + 150 + 3* cellSize);

        addBreadCrumb("[0-n]",0,0,numCells)
    })
}

// Update the breadcrumb trail to show the current sequence and percentage.
function addBreadCrumb(crumbData,start,stop,numcells) {
    num_crumbs = $('.crumb').length
    var crumb = d3.select(".breadcrumb")
        .append("li")
        .attr("id",num_crumbs+1)
        .attr("class","crumb")
        .append("a")
        .attr("id",num_crumbs+1)
        .attr("href","#")
        .attr("start",start)
        .attr("stop",stop)
        .attr("numcells",numcells)
        .on("click", function(d) {
            curId = (this.attributes[0].value)
            var url = dataUrlPrefix + "?start="+(this.attributes[2].value)
                + "&end="+(this.attributes[3].value)
                + "&numcells="+(this.attributes[4].value)
            updateData(url);
            // remove larger breadcrumbs
            $("li").each(function() {
                if(this.id > curId) {
                    $(this).remove();
                }
            });
            d3.event.stopPropagation();
        })
        .text(crumbData)
}

function updateData(url) {
    d3.json(url, function(error,json) {
        if (error) return console.warn(error);
        data = json;
        var svg = d3.select("svg");

        var cells = svg.selectAll(".cell")
            .data(data.data)

        cells.style("fill", function(d) {
                if (d != null) return colors[d.quantile];
                //if (d != null) return colorScale(d.freq);
                else return "url(#diagonalHatch)";
            })

        cells.enter()
            .append("svg:rect")
            .attr("x", function(d) {
                return d.id%cellsPerRow * cellSize;
            })
            .attr("y", function(d) {
                numRows = Math.floor(d.id/cellsPerRow)
                return numRows * cellSize;
            })
            .attr("rx", 4)
            .attr("ry", 4)
            .attr("class", function(d, i, j) {
                return "cell bordered cr" + j + " cc" + i;
            })
            .attr("width", cellSize)
            .attr("height", cellSize)
            .style("fill", function(d) {
                if (d != null) return colors[d.quantile];
                //if (d != null) return colorScale(d.freq);
                else return "url(#diagonalHatch)";
            })
            .on("mouseover", function(d){
               //highlight text
               d3.select(this).classed("cell-selected",true);
               if (d.bytes_per_cell <= 500 ) {
                    d3.select("#tooltip")
                    .style("left", (d3.event.pageX+20) + "px")
                    .style("top", (d3.event.pageY-20) + "px")
                    .select("#value")
                    .html("<strong>bytes per cell:</strong> "+d.bytes_per_cell+
                        "<br/><strong>start:</strong> "+d.start+"<br/><strong>stop:</strong> "+
                        d.stop+"<br/><strong>freq:</strong> "+d.freq+"<br/>"+
                        "<strong>content:</strong> '"+escapeHtml(d.content)+"'<br/>"
                        );
               } else {
                    d3.select("#tooltip")
                    .style("left", (d3.event.pageX+20) + "px")
                    .style("top", (d3.event.pageY-20) + "px")
                    .select("#value")
                    .html("<strong>bytes per cell:</strong> "+d.bytes_per_cell+
                        "<br/><strong>start:</strong> "+d.start+"<br/><strong>stop:</strong> "+
                        d.stop+"<br/><strong>freq:</strong> "+d.freq+"<br/>"
                        );
               }
               //Show the tooltip
               d3.select("#tooltip").classed("hidden", false);
            })
            .on("mouseout", function(){
               d3.select("#tooltip").classed("hidden", true);
               d3.select(this).classed("cell-selected",false);
            })
            .on("click", function(d) {
                addBreadCrumb("["+d.start+"-"+d.stop+"]",d.start,d.stop,numCells)
                var url = dataUrlPrefix
                updateData(url+"?start="+d.start+"&end="+d.stop+"&numcells="+numCells);
                d3.event.stopPropagation();
            });

        cells.exit().remove()


    });
}
