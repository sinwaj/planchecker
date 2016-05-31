package main
    
import (
    "os"
    "fmt"
    "bytes"
    "strings"
    "net/http"
    "io/ioutil"
    "github.com/gorilla/mux"
    "github.com/pivotal-gss/planchecker/plan"
)

var indentDepth = 4

func LoadHtml(file string) string {
    // Load HTML from file
    filedata, _ := ioutil.ReadFile(file)

    // Convert to string and return
    return string(filedata)
}

func IndexHandler(w http.ResponseWriter, r *http.Request) {
    // Load HTML
    pageHtml := LoadHtml("templates/index.html")

    // Print the response
    fmt.Fprintf(w, pageHtml)
}

/*
func PlanHandler(w http.ResponseWriter, r *http.Request) {
    // Read plan ID
    vars := mux.Vars(r)
    planId := vars["planId"]

    // Print the repsonse
    fmt.Fprintf(w, "Plan %s", planId)
}
*/

func PlanPostHandler(w http.ResponseWriter, r *http.Request) {
    var err error
    var planText string
    
    // Attempt to read the uploaded file
    r.ParseMultipartForm(32 << 20)
    file, _, err := r.FormFile("uploadfile")

    if err == nil {
        // If not error then try to read from file
        defer file.Close()
        buf := new(bytes.Buffer)
        n, err := buf.ReadFrom(file)
        if err != nil {
            fmt.Printf("Error reading from file upload: %s", err)
        }
        fmt.Printf("Read %d bytes from file upload", n)
        planText = buf.String()

    } else {
        // Else get the plan from POST textarea
        planText = r.FormValue("plantext")
    }

    // Create new explain object
    var explain plan.Explain

    // Init the explain from string
    err = explain.InitFromString(planText, true)
    if err != nil {
        fmt.Fprintf(w, "%s\n", err)
        return
    }

    // Generate the plan HTML
    //planHtml := explain.PrintPlanHtml()
    planHtml := RenderExplainHtml(&explain)

    // Load HTML page
    pageHtml := LoadHtml("templates/plan.html")

    // Render with the plan HTML
    fmt.Fprintf(w, pageHtml, planHtml)
}

// Render node for output to HTML
func RenderNodeHtml(n *plan.Node, indent int) string {
    HTML := ""
    indent += 1
    indentString := strings.Repeat(" ", indent * indentDepth)
    
    if n.Slice > -1 {
        HTML += fmt.Sprintf("%s   <span class=\"label label-success\">Slice %d</span>\n",
            indentString,
            n.Slice)
    }
    HTML += fmt.Sprintf("%s<strong>-> %s | startup cost %s | total cost %s | rows %d | width %d</strong>\n",
            indentString,
            n.Operator,
            n.StartupCost,
            n.TotalCost,
            n.Rows,
            n.Width)

    for _, e := range n.ExtraInfo[1:] {
        HTML += fmt.Sprintf("%s   %s\n", indentString, strings.Trim(e, " "))
    }

    for _, w := range n.Warnings {
        HTML += fmt.Sprintf("%s   <span class=\"label label-danger\">WARNING: %s | %s</span>\n", indentString, w.Cause, w.Resolution)
    }
    // Render sub nodes
    for _, s := range n.SubNodes {
        HTML += RenderNodeHtml(s, indent)
    }

    for _, s := range n.SubPlans {
        HTML += RenderPlanHtml(s, indent)
    }

    return HTML
}

// Render plan for output to console
func RenderPlanHtml(p *plan.Plan, indent int) string {
    HTML := ""
    indent += 1
    indentString := strings.Repeat(" ", indent * indentDepth)

    HTML += fmt.Sprintf("%s<strong>%s</strong>", indentString, p.Name)
    HTML += RenderNodeHtml(p.TopNode, indent)
    return HTML
}

func RenderExplainHtml(e *plan.Explain) string {
    HTML := ""
    HTML += fmt.Sprintf("<strong>Plan:</strong>\n")
    HTML += RenderNodeHtml(e.Plans[0].TopNode, 0)
    
    /*
        if node.Slice > -1 {
            fmt.Printf("%sSLICE: slice %d\n",
                thisIndent,
                node.Slice)
        }

        for _, n := range node.SubNodes {
            fmt.Printf("%sSUBNODE: %s\n", thisIndent, n.Operator)
        }

        for _, p := range node.SubPlans {
            fmt.Printf("%sSUBPLAN: %s\n", thisIndent, p.Name)
        }
    */
        /*
        fmt.Printf("%sInOut %s | Rows %f | Avg %f | Max %f | Workers %d | First %f | End %f | Offset %f\n",
            thisIndent,
            node.RowStat.InOut,
            node.RowStat.Rows,
            node.RowStat.Avg,
            node.RowStat.Max,
            node.RowStat.Workers,
            node.RowStat.First,
            node.RowStat.End,
            node.RowStat.Offset)
        */

        /*
        for _, line := range node.ExtraInfo {
            fmt.Printf("%sRAWLINE: %s\n", thisIndent, strings.Trim(line, " "))
        }
        */

    if len(e.Warnings) > 0 {
        HTML += fmt.Sprintf("<strong>Warnings:</strong>\n")
        for _, w := range e.Warnings {
            HTML += fmt.Sprintf("\t<span class=\"label label-danger\">%s | %s</span>\n", w.Cause, w.Resolution)
        }
    }

    if len(e.SliceStats) > 0 {
        HTML += fmt.Sprintf("<strong>Slice statistics:</strong>\n")
        for _, stat := range e.SliceStats {
            HTML += fmt.Sprintf("\t%s\n", stat)
        }
    }

    if e.MemoryUsed > 0 {
        HTML += fmt.Sprintf("<strong>Statement statistics:</strong>\n")
        HTML += fmt.Sprintf("\tMemory used: %d\n", e.MemoryUsed)
        HTML += fmt.Sprintf("\tMemory wanted: %d\n", e.MemoryWanted)
    }
    
    if len(e.Settings) > 0 {
        HTML += fmt.Sprintf("<strong>Settings:</strong>\n")
        for _, setting := range e.Settings {
            HTML += fmt.Sprintf("\t%s = %s\n", setting.Name, setting.Value)
        }
    }

    if e.Optimizer != "" {
        HTML += fmt.Sprintf("<strong>Optimizer status:</strong>\n")
        HTML += fmt.Sprintf("\t%s\n", e.Optimizer)
    }
    
    if e.Runtime > 0 {
        HTML += fmt.Sprintf("<strong>Total runtime:</strong>\n")
        HTML += fmt.Sprintf("\t%.0f ms\n", e.Runtime)
    }

    return HTML
}

func main() {

    port := os.Getenv("PORT")

    if port == "" {
        fmt.Println("PORT env variable not set")
        os.Exit(0)
    }

    fmt.Printf("Binding to port %s\n", port)

    // Using gorilla/mux as it provides named URL variable parsing
    r := mux.NewRouter()

    // Add handlers for each URL
    // Basic Index page
    r.HandleFunc("/", IndexHandler)

    // Reload an already submitted plan
    //r.HandleFunc("/plan/{planId}", PlanHandler)

    // Receive a POST form when user submits a new plan
    r.HandleFunc("/plan/", PlanPostHandler)

    // Start listening
    http.ListenAndServe(":"+port, r)
}
