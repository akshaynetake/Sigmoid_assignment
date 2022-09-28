const fs = require("fs");
const { parse } = require("csv-parse");

const readData = () => {
    var data = {};
    var curr
    fs.createReadStream("./1530947027582.csv")
        .pipe(parse({ delimiter: ":", from_line: 1 }))
        .on("data", function (row) {
            var val = row[0].split(",")
            if (val.length < 2 && typeof (val[0]) != 'number') {
                curr = val[0]
            } else {
                let aa = val.map(i => parseInt(i))
                if (aa[1]) {
                    if (data[checkTimeHr(aa[0])]) {
                        if (data[checkTimeHr(aa[0])][curr]) {
                            data[checkTimeHr(aa[0])][curr] += aa[1]
                        } else {
                            data[checkTimeHr(aa[0])][curr] = aa[1]
                        }
                    } else {
                        data[checkTimeHr(aa[0])] = {}
                        data[checkTimeHr(aa[0])][curr] = aa[1]
                    }
                }
            }
        })
        .on("end", function () {
            csvFormatData(data)
        })
        .on("error", function (error) {
            console.log(error.message);
        });
}

const checkTimeHr = (timstamp) => {
    let date = new Date(timstamp)
    date.setMinutes(date.getMinutes() + 59);
    date.setMinutes(0, 0, 0);
    return parseInt(date.getTime());
}

const csvFormatData = (data) => {
    var csvData = [['timestamp', 'bids', 'impression', 'wins', 'click']]
    for (var key of Object.keys(data)) {
        var tmp = Object.values(data[key])
        tmp.splice(0, 0, key)
        // for (var key1 of Object.keys(data[key])) {
        //     tmp.push[data[key][key1]]
        // }
        csvData.push(tmp)
    }
    createCsv(csvData)
}

const createCsv = (data) => {
    var csv = "";
    for (let i of data) {
        csv += i.join(",") + "\r\n";
    }

    const fs = require("fs");
    fs.writeFileSync("1530947027582_out.csv", csv);
    console.log("Done!");

}

readData()