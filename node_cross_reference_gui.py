#!/usr/bin/env python3
"""
Node Cross-Reference GUI Tool
============================

Tkinter-based GUI for the Node Cross-Reference analysis tool.
Provides file selection, progress tracking, results display, and manual review capabilities.
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
from tkinter.ttk import Notebook, Treeview
import threading
import os
from pathlib import Path
import csv
from typing import List, Optional

# Import our analysis classes
from node_cross_reference import NodeCrossReference, AnalysisResult, Ticket


class NodeCrossReferenceGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Node Cross-Reference Analysis Tool")
        self.root.geometry("1200x800")
        
        # Analysis state
        self.cross_ref = None
        self.results: List[AnalysisResult] = []
        self.tickets_file = ""
        self.report_file = ""
        
        self.setup_ui()
        
    def setup_ui(self):
        """Setup the main user interface"""
        
        # Main frame
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        main_frame.rowconfigure(4, weight=1)
        
        # File selection section
        self.setup_file_selection(main_frame)
        
        # Analysis controls
        self.setup_analysis_controls(main_frame)
        
        # Progress section
        self.setup_progress_section(main_frame)
        
        # Results notebook
        self.setup_results_section(main_frame)
        
        # Status bar
        self.setup_status_bar(main_frame)
        
    def setup_file_selection(self, parent):
        """Setup file selection widgets"""
        
        # File Selection Group
        file_group = ttk.LabelFrame(parent, text="Input Files", padding="10")
        file_group.grid(row=0, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        file_group.columnconfigure(1, weight=1)
        
        # Tickets CSV file
        ttk.Label(file_group, text="Tickets CSV:").grid(row=0, column=0, sticky=tk.W, padx=(0, 10))
        self.tickets_var = tk.StringVar()
        self.tickets_entry = ttk.Entry(file_group, textvariable=self.tickets_var, state="readonly")
        self.tickets_entry.grid(row=0, column=1, sticky=(tk.W, tk.E), padx=(0, 10))
        ttk.Button(file_group, text="Browse...", 
                  command=self.browse_tickets_file).grid(row=0, column=2)
        
        # Report TXT file
        ttk.Label(file_group, text="Report TXT:").grid(row=1, column=0, sticky=tk.W, padx=(0, 10), pady=(5, 0))
        self.report_var = tk.StringVar()
        self.report_entry = ttk.Entry(file_group, textvariable=self.report_var, state="readonly")
        self.report_entry.grid(row=1, column=1, sticky=(tk.W, tk.E), padx=(0, 10), pady=(5, 0))
        ttk.Button(file_group, text="Browse...", 
                  command=self.browse_report_file).grid(row=1, column=2, pady=(5, 0))
    
    def setup_analysis_controls(self, parent):
        """Setup analysis control buttons"""
        
        controls_frame = ttk.Frame(parent)
        controls_frame.grid(row=1, column=0, columnspan=2, pady=(0, 10))
        
        self.analyze_button = ttk.Button(controls_frame, text="Run Analysis", 
                                        command=self.run_analysis, state="disabled")
        self.analyze_button.pack(side=tk.LEFT, padx=(0, 10))
        
        self.export_button = ttk.Button(controls_frame, text="Export Results", 
                                       command=self.export_results, state="disabled")
        self.export_button.pack(side=tk.LEFT, padx=(0, 10))
        
        ttk.Button(controls_frame, text="Clear Results", 
                  command=self.clear_results).pack(side=tk.LEFT)
    
    def setup_progress_section(self, parent):
        """Setup progress bar and status"""
        
        progress_frame = ttk.LabelFrame(parent, text="Progress", padding="10")
        progress_frame.grid(row=2, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        progress_frame.columnconfigure(0, weight=1)
        
        self.progress_var = tk.StringVar(value="Ready")
        self.progress_label = ttk.Label(progress_frame, textvariable=self.progress_var)
        self.progress_label.grid(row=0, column=0, sticky=tk.W)
        
        self.progress_bar = ttk.Progressbar(progress_frame, mode='indeterminate')
        self.progress_bar.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(5, 0))
    
    def setup_results_section(self, parent):
        """Setup results display with tabs"""
        
        # Results notebook
        self.notebook = Notebook(parent)
        self.notebook.grid(row=4, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10))
        
        # Summary tab
        self.setup_summary_tab()
        
        # Can Close tab
        self.setup_can_close_tab()
        
        # Need Review tab
        self.setup_need_review_tab()
        
        # Errors tab
        self.setup_errors_tab()
    
    def setup_summary_tab(self):
        """Setup summary statistics tab"""
        
        summary_frame = ttk.Frame(self.notebook, padding="10")
        self.notebook.add(summary_frame, text="Summary")
        
        self.summary_text = scrolledtext.ScrolledText(summary_frame, wrap=tk.WORD, height=20)
        self.summary_text.pack(fill=tk.BOTH, expand=True)
    
    def setup_can_close_tab(self):
        """Setup can close tickets tab"""
        
        can_close_frame = ttk.Frame(self.notebook, padding="10")
        self.notebook.add(can_close_frame, text="Can Close (0)")
        
        # Treeview for can close tickets
        columns = ('Ticket', 'Store', 'Node', 'Confidence', 'Reason')
        self.can_close_tree = Treeview(can_close_frame, columns=columns, show='headings', height=15)
        
        # Configure column headings
        for col in columns:
            self.can_close_tree.heading(col, text=col)
            self.can_close_tree.column(col, width=120)
        
        # Scrollbars
        can_close_scroll_v = ttk.Scrollbar(can_close_frame, orient=tk.VERTICAL, command=self.can_close_tree.yview)
        can_close_scroll_h = ttk.Scrollbar(can_close_frame, orient=tk.HORIZONTAL, command=self.can_close_tree.xview)
        self.can_close_tree.configure(yscrollcommand=can_close_scroll_v.set, xscrollcommand=can_close_scroll_h.set)
        
        # Grid layout
        self.can_close_tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        can_close_scroll_v.grid(row=0, column=1, sticky=(tk.N, tk.S))
        can_close_scroll_h.grid(row=1, column=0, sticky=(tk.W, tk.E))
        
        can_close_frame.columnconfigure(0, weight=1)
        can_close_frame.rowconfigure(0, weight=1)
    
    def setup_need_review_tab(self):
        """Setup need review tickets tab"""
        
        review_frame = ttk.Frame(self.notebook, padding="10")
        self.notebook.add(review_frame, text="Need Review (0)")
        
        # Treeview for review tickets
        columns = ('Ticket', 'Store', 'Node', 'Confidence', 'Flag', 'Reason')
        self.review_tree = Treeview(review_frame, columns=columns, show='headings', height=15)
        
        # Configure column headings
        for col in columns:
            self.review_tree.heading(col, text=col)
            self.review_tree.column(col, width=120)
        
        # Scrollbars
        review_scroll_v = ttk.Scrollbar(review_frame, orient=tk.VERTICAL, command=self.review_tree.yview)
        review_scroll_h = ttk.Scrollbar(review_frame, orient=tk.HORIZONTAL, command=self.review_tree.xview)
        self.review_tree.configure(yscrollcommand=review_scroll_v.set, xscrollcommand=review_scroll_h.set)
        
        # Grid layout
        self.review_tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        review_scroll_v.grid(row=0, column=1, sticky=(tk.N, tk.S))
        review_scroll_h.grid(row=1, column=0, sticky=(tk.W, tk.E))
        
        review_frame.columnconfigure(0, weight=1)
        review_frame.rowconfigure(0, weight=1)
    
    def setup_errors_tab(self):
        """Setup errors tab"""
        
        errors_frame = ttk.Frame(self.notebook, padding="10")
        self.notebook.add(errors_frame, text="Errors (0)")
        
        # Treeview for errors
        columns = ('Ticket', 'Site', 'Error')
        self.errors_tree = Treeview(errors_frame, columns=columns, show='headings', height=15)
        
        # Configure column headings
        for col in columns:
            self.errors_tree.heading(col, text=col)
            self.errors_tree.column(col, width=200)
        
        # Scrollbars
        errors_scroll_v = ttk.Scrollbar(errors_frame, orient=tk.VERTICAL, command=self.errors_tree.yview)
        errors_scroll_h = ttk.Scrollbar(errors_frame, orient=tk.HORIZONTAL, command=self.errors_tree.xview)
        self.errors_tree.configure(yscrollcommand=errors_scroll_v.set, xscrollcommand=errors_scroll_h.set)
        
        # Grid layout
        self.errors_tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        errors_scroll_v.grid(row=0, column=1, sticky=(tk.N, tk.S))
        errors_scroll_h.grid(row=1, column=0, sticky=(tk.W, tk.E))
        
        errors_frame.columnconfigure(0, weight=1)
        errors_frame.rowconfigure(0, weight=1)
    
    def setup_status_bar(self, parent):
        """Setup status bar"""
        
        self.status_var = tk.StringVar(value="Ready - Select input files to begin")
        status_bar = ttk.Label(parent, textvariable=self.status_var, relief=tk.SUNKEN)
        status_bar.grid(row=5, column=0, columnspan=2, sticky=(tk.W, tk.E))
    
    def browse_tickets_file(self):
        """Browse for tickets CSV file"""
        
        filename = filedialog.askopenfilename(
            title="Select Tickets CSV File",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            initialdir=os.getcwd()
        )
        
        if filename:
            self.tickets_file = filename
            self.tickets_var.set(filename)
            self.update_analysis_button_state()
            self.status_var.set(f"Selected tickets file: {Path(filename).name}")
    
    def browse_report_file(self):
        """Browse for report TXT file"""
        
        filename = filedialog.askopenfilename(
            title="Select Report TXT File",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
            initialdir=os.getcwd()
        )
        
        if filename:
            self.report_file = filename
            self.report_var.set(filename)
            self.update_analysis_button_state()
            self.status_var.set(f"Selected report file: {Path(filename).name}")
    
    def update_analysis_button_state(self):
        """Enable analysis button when both files are selected"""
        
        if self.tickets_file and self.report_file:
            self.analyze_button.config(state="normal")
            self.status_var.set("Ready to run analysis")
        else:
            self.analyze_button.config(state="disabled")
    
    def run_analysis(self):
        """Run the analysis in a separate thread"""
        
        if not self.tickets_file or not self.report_file:
            messagebox.showerror("Error", "Please select both input files")
            return
        
        # Disable controls during analysis
        self.analyze_button.config(state="disabled")
        self.export_button.config(state="disabled")
        
        # Start progress animation
        self.progress_bar.start(10)
        self.progress_var.set("Loading data...")
        
        # Run analysis in separate thread
        analysis_thread = threading.Thread(target=self._run_analysis_thread)
        analysis_thread.daemon = True
        analysis_thread.start()
    
    def _run_analysis_thread(self):
        """Analysis thread function"""
        
        try:
            # Initialize cross-reference analyzer
            self.cross_ref = NodeCrossReference()
            
            # Update progress
            self.root.after(0, lambda: self.progress_var.set("Loading tickets..."))
            
            # Load data
            self.cross_ref.load_tickets(self.tickets_file)
            
            self.root.after(0, lambda: self.progress_var.set("Loading offline nodes report..."))
            self.cross_ref.load_offline_nodes(self.report_file)
            
            self.root.after(0, lambda: self.progress_var.set("Analyzing tickets..."))
            
            # Analyze tickets
            self.cross_ref.analyze_all_tickets()
            
            # Update UI with results
            self.root.after(0, self._analysis_complete)
            
        except Exception as e:
            error_msg = f"Analysis failed: {str(e)}"
            self.root.after(0, lambda: self._analysis_error(error_msg))
    
    def _analysis_complete(self):
        """Called when analysis is complete"""
        
        # Stop progress animation
        self.progress_bar.stop()
        self.progress_var.set("Analysis complete")
        
        # Get results
        self.results = self.cross_ref.results
        
        # Update UI
        self.update_results_display()
        
        # Re-enable controls
        self.analyze_button.config(state="normal")
        self.export_button.config(state="normal")
        
        # Update status
        can_close = len([r for r in self.results if r.status == "can_close"])
        needs_review = len([r for r in self.results if r.status == "needs_review"])
        errors = len([r for r in self.results if r.status == "error"])
        
        self.status_var.set(f"Analysis complete: {can_close} can close, {needs_review} need review, {errors} errors")
    
    def _analysis_error(self, error_msg):
        """Called when analysis fails"""
        
        # Stop progress animation
        self.progress_bar.stop()
        self.progress_var.set("Analysis failed")
        
        # Show error
        messagebox.showerror("Analysis Error", error_msg)
        
        # Re-enable controls
        self.analyze_button.config(state="normal")
        
        self.status_var.set("Analysis failed - check error message")
    
    def update_results_display(self):
        """Update the results display with analysis results"""
        
        if not self.results:
            return
        
        # Clear existing data
        self.can_close_tree.delete(*self.can_close_tree.get_children())
        self.review_tree.delete(*self.review_tree.get_children())
        self.errors_tree.delete(*self.errors_tree.get_children())
        
        # Count results by category
        can_close_count = 0
        review_count = 0
        error_count = 0
        
        # Populate tables
        for result in self.results:
            ticket = result.ticket
            
            if result.status == "can_close":
                self.can_close_tree.insert('', 'end', values=(
                    ticket.number,
                    ticket.store_number or "N/A",
                    ticket.node_number or "N/A", 
                    result.confidence,
                    result.reason[:50] + "..." if len(result.reason) > 50 else result.reason
                ))
                can_close_count += 1
                
            elif result.status == "needs_review":
                self.review_tree.insert('', 'end', values=(
                    ticket.number,
                    ticket.store_number or "N/A",
                    ticket.node_number or "N/A",
                    result.confidence,
                    result.business_logic_flag,
                    result.reason[:50] + "..." if len(result.reason) > 50 else result.reason
                ))
                review_count += 1
                
            elif result.status == "error":
                self.errors_tree.insert('', 'end', values=(
                    ticket.number,
                    ticket.site[:30] + "..." if len(ticket.site) > 30 else ticket.site,
                    result.reason
                ))
                error_count += 1
        
        # Update tab titles
        tabs = list(self.notebook.tabs())
        self.notebook.tab(tabs[1], text=f"Can Close ({can_close_count})")
        self.notebook.tab(tabs[2], text=f"Need Review ({review_count})")  
        self.notebook.tab(tabs[3], text=f"Errors ({error_count})")
        
        # Update summary
        self.update_summary()
    
    def update_summary(self):
        """Update summary statistics"""
        
        if not self.results:
            return
        
        summary = []
        summary.append("NODE CROSS-REFERENCE ANALYSIS SUMMARY")
        summary.append("=" * 50)
        summary.append("")
        
        total = len(self.results)
        can_close = len([r for r in self.results if r.status == "can_close"])
        needs_review = len([r for r in self.results if r.status == "needs_review"])
        errors = len([r for r in self.results if r.status == "error"])
        
        summary.append("OVERALL STATISTICS:")
        summary.append(f"Total tickets analyzed: {total}")
        summary.append(f"Can close: {can_close} ({can_close/total*100:.1f}%)")
        summary.append(f"Need review: {needs_review} ({needs_review/total*100:.1f}%)")
        summary.append(f"Errors: {errors} ({errors/total*100:.1f}%)")
        summary.append("")
        
        # Confidence breakdown
        high_conf = len([r for r in self.results if r.confidence == "high"])
        med_conf = len([r for r in self.results if r.confidence == "medium"])
        low_conf = len([r for r in self.results if r.confidence == "low"])
        
        summary.append("CONFIDENCE BREAKDOWN:")
        summary.append(f"High confidence: {high_conf} ({high_conf/total*100:.1f}%)")
        summary.append(f"Medium confidence: {med_conf} ({med_conf/total*100:.1f}%)")
        summary.append(f"Low confidence: {low_conf} ({low_conf/total*100:.1f}%)")
        summary.append("")
        
        # Business logic flags
        business_flagged = len([r for r in self.results if r.business_logic_flag])
        summary.append("BUSINESS LOGIC FLAGS:")
        summary.append(f"Tickets with business logic flags: {business_flagged} ({business_flagged/total*100:.1f}%)")
        
        if business_flagged > 0:
            flag_counts = {}
            for result in self.results:
                if result.business_logic_flag:
                    flag_counts[result.business_logic_flag] = flag_counts.get(result.business_logic_flag, 0) + 1
            for flag, count in flag_counts.items():
                summary.append(f"  {flag}: {count} tickets")
        
        # Set summary text
        self.summary_text.delete(1.0, tk.END)
        self.summary_text.insert(1.0, "\n".join(summary))
    
    def export_results(self):
        """Export results to CSV files"""
        
        if not self.results:
            messagebox.showwarning("Warning", "No results to export")
            return
        
        try:
            # Use the cross_ref object's export method
            self.cross_ref.export_results()
            
            # Show success message
            files_created = []
            if any(r.status == "can_close" for r in self.results):
                files_created.append("results_can_close.csv")
            if any(r.status == "needs_review" for r in self.results):
                files_created.append("results_need_review.csv")
            if any(r.status == "error" for r in self.results):
                files_created.append("results_errors.csv")
            files_created.append("summary_report.txt")
            
            message = "Results exported successfully!\n\nFiles created:\n" + "\n".join(f"• {f}" for f in files_created)
            messagebox.showinfo("Export Complete", message)
            
            self.status_var.set(f"Exported {len(files_created)} result files")
            
        except Exception as e:
            messagebox.showerror("Export Error", f"Failed to export results: {str(e)}")
    
    def clear_results(self):
        """Clear all results and reset the interface"""
        
        # Clear data
        self.results = []
        self.cross_ref = None
        
        # Clear UI
        self.can_close_tree.delete(*self.can_close_tree.get_children())
        self.review_tree.delete(*self.review_tree.get_children())
        self.errors_tree.delete(*self.errors_tree.get_children())
        self.summary_text.delete(1.0, tk.END)
        
        # Reset tab titles
        tabs = list(self.notebook.tabs())
        self.notebook.tab(tabs[1], text="Can Close (0)")
        self.notebook.tab(tabs[2], text="Need Review (0)")
        self.notebook.tab(tabs[3], text="Errors (0)")
        
        # Reset progress
        self.progress_bar.stop()
        self.progress_var.set("Ready")
        
        # Disable export
        self.export_button.config(state="disabled")
        
        # Update status
        self.status_var.set("Results cleared")


def main():
    """Main function to run the GUI application"""
    
    root = tk.Tk()
    app = NodeCrossReferenceGUI(root)
    
    # Center the window
    root.update_idletasks()
    x = (root.winfo_screenwidth() // 2) - (root.winfo_width() // 2)
    y = (root.winfo_screenheight() // 2) - (root.winfo_height() // 2)
    root.geometry(f"+{x}+{y}")
    
    root.mainloop()


if __name__ == "__main__":
    main()